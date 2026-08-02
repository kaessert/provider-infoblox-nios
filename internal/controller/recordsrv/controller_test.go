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
	cperrors "github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordsrv/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordsrv/v1alpha1"
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

// testUIDCluster and testUIDNamespaced are the fixed metadata.uid values
// the CR builders stamp onto their fixture CRs. Tests that seed a WAPI
// record already carrying the provider's identity extensible attribute
// (identity.Stamp) use these constants so the fixture's stamped uid
// matches the CR's own uid — the identity ladder's "steady state"
// (identity.OutcomeResolved) — unless a test is specifically exercising
// adoption, rotation, or a foreign-owned object.
const (
	testUIDCluster    = "test-uid-cluster"
	testUIDNamespaced = "test-uid-namespaced"
)

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
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
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
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
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

	// searchCalls counts requests to the search endpoint (a GET with no
	// _ref path segment) — used to prove the identity ladder actually
	// issued a round trip rather than short-circuiting.
	searchCalls int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte

	// ── identity EA-definition prerequisite probe state ─────────────
	//
	// eaDefExists controls whether GET .../extensibleattributedef
	// reports the identity extensible attribute definition as present.
	// Defaults to true (see newMockWapiServer) so tests that do not
	// specifically exercise the prerequisite probe never trigger a
	// create call for it.
	eaDefExists bool
	// eaDefCreateStatus, when non-zero, is the HTTP status the mock
	// returns for a POST .../extensibleattributedef instead of
	// succeeding — used to simulate a credential that cannot create the
	// definition (401/403).
	eaDefCreateStatus int
	// eaDefCreateBody is the response body written alongside
	// eaDefCreateStatus — a WAPI-shaped error payload.
	eaDefCreateBody string
	// eaDefSearchCalls/eaDefCreateCalls count requests to the
	// extensibleattributedef existence-check and create endpoints,
	// independent of searchCalls above.
	eaDefSearchCalls int
	eaDefCreateCalls int

	// undefinedEASearch simulates a Grid where the identity extensible
	// attribute definition itself does not exist: a GET search filtered
	// by "*<EA name>" returns HTTP 400 ("AdmConProtoError: Unknown
	// extensible attribute: ..."), instead of the ordinary empty-array
	// "no matches" response. Only the identity-EA search path (a filter
	// key prefixed with "*") is affected.
	undefinedEASearch bool

	// createCalls/putCalls count POST/PUT requests against record:srv
	// itself (independent of eaDefCreateCalls above), used to prove a
	// Create call issues exactly one mutating request — no follow-up PUT
	// to re-assert the identity stamp — and that a refused Create/Update
	// issues zero of either.
	createCalls int
	putCalls    int
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{
		records: map[string]*ibclient.RecordSRV{},
		// The identity EA definition is present by default so every
		// pre-existing Create test sees the prerequisite as already
		// satisfied and never exercises the create-definition path.
		eaDefExists: true,
	}
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
		m.mu.Lock()
		m.createCalls++
		m.mu.Unlock()
		// Synthesize the zone the way NIOS derives it server-side (last
		// two labels of the FQDN), so Observe/Create tests can assert
		// the response-only Zone field is mirrored.
		rec.Zone = zoneFromName(rec.Name)
		ref := m.seed(&rec)
		writeJSON(w, http.StatusOK, ref)
	})

	// Search endpoint used by GetSRVRecord's natural-key fallback, filtered
	// by view/name/target/port query params. Registered as an exact
	// literal path so Go's ServeMux prefers it over the {ref...} wildcard
	// below for requests to precisely "record:srv" (real _refs always
	// carry additional path segments).
	// Identity EA-definition prerequisite probe endpoints
	// (internal/clients/identity.Prober.Ensure): the existence check and,
	// when absent, the create attempt for the "Crossplane Internal ID"
	// extensible attribute definition. eaDefExists defaults to true (see
	// newMockWapiServer) so tests that never touch these fields see the
	// prerequisite as already satisfied.
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.eaDefSearchCalls++
		exists := m.eaDefExists
		m.mu.Unlock()

		if !exists {
			writeJSON(w, http.StatusOK, []ibclient.EADefinition{})
			return
		}
		name := identity.EAKey
		writeJSON(w, http.StatusOK, []ibclient.EADefinition{{Name: &name}})
	})

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.eaDefCreateCalls++
		status := m.eaDefCreateStatus
		body := m.eaDefCreateBody
		m.mu.Unlock()

		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}

		m.mu.Lock()
		m.eaDefExists = true
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, "extensibleattributedef/test:"+url.QueryEscape(identity.EAKey))
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/record:srv", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.searchCalls++
		m.mu.Unlock()

		q := r.URL.Query()
		view := q.Get("view")
		name := q.Get("name")
		target := q.Get("target")
		port := q.Get("port")

		eaFilters := map[string]string{}
		for k, vals := range q {
			if strings.HasPrefix(k, "*") && len(vals) > 0 {
				eaFilters[strings.TrimPrefix(k, "*")] = vals[0]
			}
		}

		m.mu.Lock()
		undefinedEA := m.undefinedEASearch
		m.mu.Unlock()
		if len(eaFilters) > 0 && undefinedEA {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"Error":"AdmConProtoError: Unknown extensible attribute: ` + identity.EAKey + `","code":"Client.Ibap.Proto","text":"Unknown extensible attribute: ` + identity.EAKey + `"}`))
			return
		}

		m.mu.Lock()
		var matches []ibclient.RecordSRV
		for _, rec := range m.records {
			if view != "" && rec.View != view {
				continue
			}
			if name != "" && (rec.Name == nil || *rec.Name != name) {
				continue
			}
			if target != "" && (rec.Target == nil || *rec.Target != target) {
				continue
			}
			if port != "" && (rec.Port == nil || itoa(int(*rec.Port)) != port) {
				continue
			}
			eaMismatch := false
			for k, v := range eaFilters {
				got, ok := rec.Ea[k]
				if !ok {
					eaMismatch = true
					break
				}
				if s, ok := got.(string); !ok || s != v {
					eaMismatch = true
					break
				}
			}
			if eaMismatch {
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
		var incoming ibclient.RecordSRV
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		m.putCalls++

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
func newTestObjectManager(t *testing.T, srv *httptest.Server) identity.ManagerAndConnector {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	mgrConn, err := newObjectManagerWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test object manager: %v", err)
	}
	return mgrConn
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
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/does-not-exist:_sip._tcp.example.com/default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestClusterObserveRefInstabilityFallsBackToSearch verifies that Observe
// recovers from a stale external-name annotation (the stored _ref no
// longer resolves because a prior Update rotated it and the refreshed
// annotation was never persisted — e.g. a crash between the WAPI write
// succeeding and the annotation Patch landing) by re-searching via the
// CR's own identity fields (view, name, target, port), and refreshes the
// external name to the record's current _ref. This is the defense in
// depth called for by the external-name-refresh-conflict-safety fix:
// even a conflict-proof persist has a crash window, and this fallback is
// what turns that window from "wedged forever" into "self-heals on the
// next reconcile".
func TestClusterObserveRefInstabilityFallsBackToSearch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	realRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	// External-name annotation points at a stale _ref (as if the record
	// was renamed by a prior reconcile that changed identity fields, but
	// the annotation refresh was lost — e.g. controller restart).
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/stale:old.example.com/default")

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
// call) when the external-name still equals the CR's Kubernetes name —
// the pre-create state for a server-assigned external-name strategy.
func TestObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())        // simulate NameAsExternalName initializer

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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/test1:_sip._tcp.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	kube := &recordingKubeClient{}
	e := &clusterExternal{kube: kube, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", oldRef)
	cr.Spec.ForProvider.Name = stringPtr("_sip._tcp.renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating field update, want a refreshed _ref")
	}

	// Regression guard: the refreshed external-name must be persisted via
	// a real kube client call, not merely mutated in memory on cr. The
	// managed reconciler only flushes the status subresource after a
	// successful external Update() — any annotation change that isn't
	// pushed through kube.Update() here is silently discarded, and the
	// very next Observe() would use the stale, now-404ing ref.
	if kube.updated == nil {
		t.Fatal("Update: external-name refresh was not persisted via kube.Update — the annotation change would be silently discarded by the managed reconciler")
	}
	if got := meta.GetExternalName(kube.updated); got != newRef {
		t.Errorf("Update: persisted external-name = %q, want %q", got, newRef)
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	ref := m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.example.com"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/test1:_sip._tcp.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteSRVRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteSRVRecord)
	}
}

// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject verifies the
// core defect fix: a 404 against the stored _ref must not be treated as
// "already deleted" when a natural-key search finds the same identity
// still live under a different _ref. Deleting that record would be
// unverifiable ownership, so Delete() must refuse and leave the record in
// place.
func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		View:     "default",
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error recovering a rotated object via identity search: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: recovered object still present after Delete")
	}
}

// TestClusterDeleteRefusesOnForeignIdentity verifies the identity
// ladder's ownership check: when the stored _ref resolves directly to an
// object whose identity extensible attribute names a different owner,
// Delete() must refuse rather than destroy someone else's object.
func TestClusterDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		View:     "default",
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", foreignRef)

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Delete: error = %v, want it to wrap a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[foreignRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: foreign record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// companion happy path: a 404 against the stored _ref, and an
// identity-EA search that finds nothing, means the object really is
// gone.
func TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
	}
}

// TestClusterObserveRefusesOnForeignIdentity verifies that Observe
// surfaces a HandleReuseError (Synced=False, no mutating call) when the
// stored _ref resolves to an object whose identity attribute belongs to
// a different owner.
func TestClusterObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		View:     "default",
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", foreignRef)

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Observe: error = %v, want it to wrap a *identity.HandleReuseError", err)
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/does-not-exist:_sip._tcp.example.com/default", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestNamespacedObserveRefInstabilityFallsBackToSearch is the namespaced
// counterpart of TestClusterObserveRefInstabilityFallsBackToSearch — see
// that test's doc comment for the full rationale.
func TestNamespacedObserveRefInstabilityFallsBackToSearch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	realRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/stale:old.example.com/default", "ProviderConfig")

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

func TestNamespacedObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "", "ProviderConfig")
	meta.SetExternalName(cr, cr.GetName())

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

func TestNamespacedObserveServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/test1:_sip._tcp.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	ref := m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.example.com"), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/test1:_sip._tcp.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteSRVRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteSRVRecord)
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		View:     "default",
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error recovering a rotated object via identity search: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: recovered object still present after Delete")
	}
}

// TestNamespacedDeleteRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterDeleteRefusesOnForeignIdentity.
func TestNamespacedDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		View:     "default",
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", foreignRef, "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Delete: error = %v, want it to wrap a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[foreignRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: foreign record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// namespaced-scope counterpart of
// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch.
func TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
	}
}

// TestNamespacedObserveRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterObserveRefusesOnForeignIdentity.
func TestNamespacedObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		View:     "default",
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", foreignRef, "ProviderConfig")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Observe: error = %v, want it to wrap a *identity.HandleReuseError", err)
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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
			mgrConn, err := newObjectManagerWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
			}
			if mgrConn.Manager == nil || mgrConn.Connector == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil manager and connector")
			}
		})
	}
}

// ── Identity: stamp isolation from spec.forProvider ─────────────────────

// TestLateInitializeStripsIdentityEAFromExtAttrs proves the reserved
// identity extensible attribute (identity.EAKey) never leaks into
// spec.forProvider.extAttrs via late-init — the CRD schema's CEL rule
// rejects a user-supplied value for that key, so back-filling it would
// permanently break the resource.
func TestLateInitializeStripsIdentityEAFromExtAttrs(t *testing.T) {
	var comment *string
	var ttl *uint32
	var useTTL *bool
	extAttrs := map[string]string(nil)

	rec := &ibclient.RecordSRV{
		Ea: identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	}

	changed := lateInitialize(&comment, &ttl, &useTTL, &extAttrs, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if _, present := extAttrs[identity.EAKey]; present {
		t.Errorf("lateInitialize: extAttrs = %v, must not contain the reserved identity key %q", extAttrs, identity.EAKey)
	}
	if !extAttrsEqual(extAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("lateInitialize: extAttrs = %v, want {env: prod}", extAttrs)
	}
}

// TestIsUpToDateIgnoresIdentityEA proves isUpToDate compares extAttrs
// with the identity stamp stripped, so an object freshly stamped by
// Create/Update never appears out of date merely because the Grid's
// extattrs map carries a key the CRD schema does not expose.
func TestIsUpToDateIgnoresIdentityEA(t *testing.T) {
	rec := &ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	}

	got := isUpToDate(stringPtr("_sip._tcp.example.com"), stringPtr("sipserver.example.com"), nil, uint32Ptr(10), uint32Ptr(20), uint32Ptr(5060), nil, nil, map[string]string{"env": "prod"}, rec)
	if !got {
		t.Error("isUpToDate: want true when spec.forProvider.extAttrs matches the Grid map with the identity stamp stripped, got false")
	}
}

// TestClusterObserveAtProviderExtAttrsIncludesIdentityKey proves
// status.atProvider.extAttrs mirrors the Grid's full extattrs map,
// identity stamp included.
func TestClusterObserveAtProviderExtAttrsIncludesIdentityKey(t *testing.T) {
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
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", ref)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; got != testUIDCluster {
		t.Errorf("AtProvider.ExtAttrs[%q] = %q, want %q (full Grid EA mirror, stamp included)", identity.EAKey, got, testUIDCluster)
	}
}

// ── Identity: empty-uid refusal ──────────────────────────────────────────

func TestCreateSRVRecordRefusesEmptyUID(t *testing.T) {
	_, err := createSRVRecord(nil, "default", stringPtr("_sip._tcp.example.com"), stringPtr("sipserver.example.com"), nil, uint32Ptr(10), uint32Ptr(20), uint32Ptr(5060), nil, nil, nil, "")
	if err == nil {
		t.Fatal("createSRVRecord: expected an error for an empty uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("createSRVRecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateSRVRecordRefusesEmptyUID(t *testing.T) {
	_, err := updateSRVRecord(nil, "record:srv/test1:_sip._tcp.example.com/default", stringPtr("_sip._tcp.example.com"), stringPtr("sipserver.example.com"), nil, uint32Ptr(10), uint32Ptr(20), uint32Ptr(5060), nil, nil, nil, "")
	if err == nil {
		t.Fatal("updateSRVRecord: expected an error for an empty uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("updateSRVRecord: error = %v, want it to mention the empty uid", err)
	}
}

// TestCreateSRVRecordRefusesWhitespaceUID and
// TestUpdateSRVRecordRefusesWhitespaceUID: a whitespace-only uid is not
// empty by a literal "" comparison, but it is not a usable identity
// either — the guard must trim before checking, matching the shared
// identity resolution ladder's own TrimSpace check.

func TestCreateSRVRecordRefusesWhitespaceUID(t *testing.T) {
	_, err := createSRVRecord(nil, "default", stringPtr("_sip._tcp.example.com"), stringPtr("sipserver.example.com"), nil, uint32Ptr(10), uint32Ptr(20), uint32Ptr(5060), nil, nil, nil, "   ")
	if err == nil {
		t.Fatal("createSRVRecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("createSRVRecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateSRVRecordRefusesWhitespaceUID(t *testing.T) {
	_, err := updateSRVRecord(nil, "record:srv/test1:_sip._tcp.example.com/default", stringPtr("_sip._tcp.example.com"), stringPtr("sipserver.example.com"), nil, uint32Ptr(10), uint32Ptr(20), uint32Ptr(5060), nil, nil, nil, "   ")
	if err == nil {
		t.Fatal("updateSRVRecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("updateSRVRecord: error = %v, want it to mention the empty uid", err)
	}
}

// ── identity ladder: every row, both scopes ─────────────────────────────
//
// What follows fills the remaining identity-ladder rows not already
// exercised above: Rotated, Adopted, FoundByUID, AmbiguousMatchError, and
// the namespaced HandleReuseError row — so no ladder outcome is
// exercised on only one scope.

// TestClusterObserveRecoversRotatedRefAndPersistsAnnotation is the
// Rotated row: the stored _ref 404s (NIOS minted a new _ref because a
// _ref-mutating field changed out from under the stale annotation), and
// the object is relocated purely by its stamped identity attribute.
func TestClusterObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for a rotated object recovered by identity search, got false")
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true so the refreshed reference is persisted, got false")
	}
	if got := meta.GetExternalName(cr); got != newRef {
		t.Errorf("Observe: external-name = %q, want the recovered reference %q", got, newRef)
	}

	m.mu.Lock()
	_, stillExists := m.records[newRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live record was removed — Observe() must never mutate the backend")
	}
}

func TestNamespacedObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for a rotated object recovered by identity search, got false")
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true so the refreshed reference is persisted, got false")
	}
	if got := meta.GetExternalName(cr); got != newRef {
		t.Errorf("Observe: external-name = %q, want the recovered reference %q", got, newRef)
	}

	m.mu.Lock()
	_, stillExists := m.records[newRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live record was removed — Observe() must never mutate the backend")
	}
}

func TestClusterObserveAdoptsUnstampedObjectAndForcesUpdate(t *testing.T) {
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
		// No Ea at all — the object has never been stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for an adopted object, got false")
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false for an adopted (unstamped) object even though every other field matches, got true — the identity stamp would never be applied")
	}
}

func TestNamespacedObserveAdoptsUnstampedObjectAndForcesUpdate(t *testing.T) {
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
		// No Ea at all — the object has never been stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for an adopted object, got false")
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false for an adopted (unstamped) object even though every other field matches, got true — the identity stamp would never be applied")
	}
}

// TestClusterObserveEmptyExternalNameRecoversSingleMatch is the
// FoundByUID row: no external-name has ever been assigned (the
// pre-create NameAsExternalName state), and the object is located purely
// by its stamped identity attribute. Closes the create-crash window —
// see convention 0107.
func TestClusterObserveEmptyExternalNameRecoversSingleMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foundRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "")
	meta.SetExternalName(cr, cr.GetName()) // simulate the NameAsExternalName pre-create state

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true — the object must be locatable purely by its stamped identity attribute with zero prior state, closing the create-crash window")
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true so the recovered reference is persisted through the path crossplane-runtime actually writes back")
	}
	if got := meta.GetExternalName(cr); got != foundRef {
		t.Errorf("Observe: external-name = %q, want the recovered reference %q", got, foundRef)
	}

	m.mu.Lock()
	searchCalls := m.searchCalls
	m.mu.Unlock()
	if searchCalls == 0 {
		t.Error("Observe: want the identity ladder to have issued a search, got zero search calls")
	}
}

func TestNamespacedObserveEmptyExternalNameRecoversSingleMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foundRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "", "ProviderConfig")
	meta.SetExternalName(cr, cr.GetName())

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true for a namespaced resource located purely by its stamped identity attribute, got false")
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true so the recovered reference is persisted")
	}
	if got := meta.GetExternalName(cr); got != foundRef {
		t.Errorf("Observe: external-name = %q, want the recovered reference %q", got, foundRef)
	}
}

// ── identity ladder: ambiguous match refusal (Observe + Delete, both scopes) ──

func TestClusterObserveRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.host-a.example.com"), Target: stringPtr("sipserver.example.com"), Priority: uint32Ptr(10), Weight: uint32Ptr(20), Port: uint32Ptr(5060), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.host-b.example.com"), Target: stringPtr("sipserver.example.com"), Priority: uint32Ptr(10), Weight: uint32Ptr(20), Port: uint32Ptr(5060), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the identity-EA search matches more than one object, got nil")
	}
	var ambiguous *identity.AmbiguousMatchError
	if !cperrors.As(err, &ambiguous) {
		t.Errorf("Observe: error = %v, want it to wrap a *identity.AmbiguousMatchError", err)
	}
}

func TestNamespacedObserveRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.host-a.example.com"), Target: stringPtr("sipserver.example.com"), Priority: uint32Ptr(10), Weight: uint32Ptr(20), Port: uint32Ptr(5060), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})
	m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.host-b.example.com"), Target: stringPtr("sipserver.example.com"), Priority: uint32Ptr(10), Weight: uint32Ptr(20), Port: uint32Ptr(5060), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default", "ProviderConfig")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the identity-EA search matches more than one object, got nil")
	}
	var ambiguous *identity.AmbiguousMatchError
	if !cperrors.As(err, &ambiguous) {
		t.Errorf("Observe: error = %v, want it to wrap a *identity.AmbiguousMatchError", err)
	}
}

func TestClusterDeleteRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	refA := m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.host-a.example.com"), Target: stringPtr("sipserver.example.com"), Priority: uint32Ptr(10), Weight: uint32Ptr(20), Port: uint32Ptr(5060), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})
	refB := m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.host-b.example.com"), Target: stringPtr("sipserver.example.com"), Priority: uint32Ptr(10), Weight: uint32Ptr(20), Port: uint32Ptr(5060), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected an error when the identity-EA search matches more than one object, got nil")
	}
	var ambiguous *identity.AmbiguousMatchError
	if !cperrors.As(err, &ambiguous) {
		t.Errorf("Delete: error = %v, want it to wrap a *identity.AmbiguousMatchError", err)
	}

	m.mu.Lock()
	_, aExists := m.records[refA]
	_, bExists := m.records[refB]
	m.mu.Unlock()
	if !aExists || !bExists {
		t.Error("Delete: an ambiguously-matched record was removed despite the refusal — DELETE must not have been issued against either candidate")
	}
}

func TestNamespacedDeleteRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	refA := m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.host-a.example.com"), Target: stringPtr("sipserver.example.com"), Priority: uint32Ptr(10), Weight: uint32Ptr(20), Port: uint32Ptr(5060), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})
	refB := m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.host-b.example.com"), Target: stringPtr("sipserver.example.com"), Priority: uint32Ptr(10), Weight: uint32Ptr(20), Port: uint32Ptr(5060), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected an error when the identity-EA search matches more than one object, got nil")
	}
	var ambiguous *identity.AmbiguousMatchError
	if !cperrors.As(err, &ambiguous) {
		t.Errorf("Delete: error = %v, want it to wrap a *identity.AmbiguousMatchError", err)
	}

	m.mu.Lock()
	_, aExists := m.records[refA]
	_, bExists := m.records[refB]
	m.mu.Unlock()
	if !aExists || !bExists {
		t.Error("Delete: an ambiguously-matched record was removed despite the refusal — DELETE must not have been issued against either candidate")
	}
}

// ── Delete's stricter policy on an unstamped (adopted) object ──────────

func TestClusterDeleteRefusesOnUnstampedObject(t *testing.T) {
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
		// No Ea at all — never stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", ref)

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected a refusal for an object with no identity stamp at all, got nil")
	}
	if !strings.Contains(err.Error(), "ownership cannot be verified") {
		t.Errorf("Delete: error = %v, want it to explain that ownership cannot be verified", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: unstamped record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

func TestNamespacedDeleteRefusesOnUnstampedObject(t *testing.T) {
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
		// No Ea at all — never stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", ref, "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected a refusal for an object with no identity stamp at all, got nil")
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: unstamped record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// ── Create stamps identity: exactly one request, asserted on the wire ───
//
// Convention 0107 forbids asserting external-name/identity effects by
// inspecting the in-memory managed resource. These tests assert on what
// the mock backend actually received (m.records, decoded straight off
// the POST body) and on the mock's request counters — never on cr.

func TestClusterCreateStampsIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	m.mu.Lock()
	stored := m.records[got]
	createCalls, putCalls := m.createCalls, m.putCalls
	m.mu.Unlock()
	if stored == nil {
		t.Fatalf("Create: no record stored under external-name %q", got)
	}
	if uid, ok := stored.Ea[identity.EAKey]; !ok || uid != string(cr.GetUID()) {
		t.Errorf("Create: stored identity EA (captured off the POST body) = %v, want %q = %q", stored.Ea, identity.EAKey, cr.GetUID())
	}
	if createCalls != 1 {
		t.Errorf("Create: POST /record:srv calls = %d, want exactly 1", createCalls)
	}
	if putCalls != 0 {
		t.Errorf("Create: PUT calls = %d, want 0 — the identity stamp must land in the same request that creates the object, no follow-up PUT", putCalls)
	}
}

func TestNamespacedCreateStampsIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	m.mu.Lock()
	stored := m.records[got]
	createCalls, putCalls := m.createCalls, m.putCalls
	m.mu.Unlock()
	if stored == nil {
		t.Fatalf("Create: no record stored under external-name %q", got)
	}
	if uid, ok := stored.Ea[identity.EAKey]; !ok || uid != string(cr.GetUID()) {
		t.Errorf("Create: stored identity EA (captured off the POST body) = %v, want %q = %q", stored.Ea, identity.EAKey, cr.GetUID())
	}
	if createCalls != 1 {
		t.Errorf("Create: POST /record:srv calls = %d, want exactly 1", createCalls)
	}
	if putCalls != 0 {
		t.Errorf("Create: PUT calls = %d, want 0 — the identity stamp must land in the same request that creates the object, no follow-up PUT", putCalls)
	}
}

// TestCreateSRVRecordRefusesEmptyUIDIssuesNoMutatingCall is the
// controller-level (not just the bare-function) companion of
// TestCreateSRVRecordRefusesEmptyUID: proves the httptest server records
// zero mutating requests when Create is refused for an empty uid.
func TestCreateSRVRecordRefusesEmptyUIDIssuesNoMutatingCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", "")
	cr.SetUID("")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected an error for a blank metadata.uid, got nil")
	}

	m.mu.Lock()
	createCalls, eaDefCreateCalls := m.createCalls, m.eaDefCreateCalls
	m.mu.Unlock()
	if createCalls != 0 {
		t.Errorf("Create: POST /record:srv calls = %d, want 0 for a refused create", createCalls)
	}
	if eaDefCreateCalls != 0 {
		t.Errorf("Create: extensibleattributedef create calls = %d, want 0 for a refused create", eaDefCreateCalls)
	}
}

// ── Update reasserts the identity stamp on every mutating call ─────────

func TestClusterUpdateReassertsIdentityStamp(t *testing.T) {
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
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterSRVRecord("my-srvrecord", ref)
	// Change only extAttrs — name/target/priority/weight/port changes
	// rotate the _ref for this resource (see the PUT handler's
	// refMutated simulation), which is already covered by
	// TestClusterUpdateRefreshesUnstableRef. This test isolates the
	// identity-reassert property from that rotation.
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored == nil {
		t.Fatal("Update: record missing after update")
	}
	if uid, ok := stored.Ea[identity.EAKey]; !ok || uid != string(cr.GetUID()) {
		t.Errorf("Update: stored identity EA = %v, want %q = %q — the PUT must re-assert the stamp on every mutating call, not just Create", stored.Ea, identity.EAKey, cr.GetUID())
	}
}

func TestNamespacedUpdateReassertsIdentityStamp(t *testing.T) {
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
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored == nil {
		t.Fatal("Update: record missing after update")
	}
	if uid, ok := stored.Ea[identity.EAKey]; !ok || uid != string(cr.GetUID()) {
		t.Errorf("Update: stored identity EA = %v, want %q = %q — the PUT must re-assert the stamp on every mutating call, not just Create", stored.Ea, identity.EAKey, cr.GetUID())
	}
}

// ── external-name refresh: round-trip through a distinct fetched object ──
//
// Convention 0107's forbidden list: a unit test that asserts external-name
// by inspecting the in-memory managed resource after Update() passes
// while the persistence bug ships. These use a real fake.Client and
// re-GET into a *distinct* object instance after Update() returns.

func TestClusterUpdateRefreshedExternalNamePersistsAcrossReGet(t *testing.T) {
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

	cr := newClusterSRVRecord("my-srvrecord", oldRef)
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr.Spec.ForProvider.Priority = uint32Ptr(99)

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating priority change, want a refreshed _ref")
	}

	fetched := &clusterv1alpha1.SRVRecord{}
	if err := kube.Get(context.Background(), client.ObjectKey{Name: cr.GetName()}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Update: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

func TestNamespacedUpdateRefreshedExternalNamePersistsAcrossReGet(t *testing.T) {
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

	cr := newNamespacedSRVRecord("default", "my-srvrecord", oldRef, "ProviderConfig")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr.Spec.ForProvider.Priority = uint32Ptr(99)

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating priority change, want a refreshed _ref")
	}

	fetched := &namespacedv1alpha1.SRVRecord{}
	if err := kube.Get(context.Background(), client.ObjectKey{Name: cr.GetName(), Namespace: cr.GetNamespace()}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Update: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

// ── identity prerequisite probe: fires only on the search failure it can
//    actually diagnose (Observe + Delete, cluster + namespaced) ─────────
//
// The probe must fire when the identity-EA search itself fails because
// the "Crossplane Internal ID" extensible-attribute definition is
// absent, and must NOT fire for any other resolution failure — a ref-GET
// failure, or either typed refusal (HandleReuseError, AmbiguousMatchError)
// — because the identity-prerequisite verdict is cached by endpoint for
// several minutes; a single mismatched probe call would poison every
// later Observe/Delete sharing that cache key for the rest of the
// window.

func TestClusterObserveSurfacesPrerequisiteErrorFromIdentitySearch(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = `{"Error":"AdmConAuthError: Not authorized"}`
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-observe-undefined-ea"}
	// No external-name ever assigned: observeRefFor reports "" for this
	// case, sending the ladder straight to the identity-EA search with
	// no ref-GET attempt first.
	cr := newClusterSRVRecord("my-srvrecord", "")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Observe: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls < 1 {
		t.Errorf("eaDefSearchCalls = %d, want at least 1 — the reactive guard must have probed", eaDefSearchCalls)
	}
}

func TestClusterObserveSteadyStateNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	m.undefinedEASearch = true // would break the ladder if reached
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-steady-state"}
	cr := newClusterSRVRecord("my-srvrecord", ref)

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error on a reference that resolves directly: %v", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — the steady-state (reference resolves) path must never probe", eaDefSearchCalls)
	}
}

func TestClusterObserveForeignIdentityNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-foreign-identity"}
	cr := newClusterSRVRecord("my-srvrecord", foreignRef)

	_, err := e.Observe(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Fatalf("Observe: error = %v, want it to wrap a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — a foreign-identity refusal has nothing to do with the identity-EA search prerequisite and must never probe", eaDefSearchCalls)
	}
}

func TestClusterObserveRefGetFailureNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// A stored _ref that resolves to a live object owned by this MR: the
	// stale-ref path below instead points at a ref that 404s AND whose
	// natural key has no match, so resolution fails for a reason
	// unrelated to the identity-EA search itself being broken — the
	// search runs (and finds nothing), which is not a search failure.
	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ref-get-failure"}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=false — the stale ref and the identity search both found nothing")
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — a clean not-found resolution must never probe the prerequisite", eaDefSearchCalls)
	}
}

func TestClusterObserveAmbiguousMatchNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.host-a.example.com"), Target: stringPtr("sipserver.example.com"), Priority: uint32Ptr(10), Weight: uint32Ptr(20), Port: uint32Ptr(5060), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.RecordSRV{Name: stringPtr("_sip._tcp.host-b.example.com"), Target: stringPtr("sipserver.example.com"), Priority: uint32Ptr(10), Weight: uint32Ptr(20), Port: uint32Ptr(5060), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ambiguous"}
	cr := newClusterSRVRecord("my-srvrecord", "record:srv/stale-ref:_sip._tcp.example.com/default")

	_, err := e.Observe(context.Background(), cr)
	var ambiguous *identity.AmbiguousMatchError
	if !cperrors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v, want it to wrap a *identity.AmbiguousMatchError", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — an ambiguous-match refusal has nothing to do with the identity-EA search prerequisite and must never probe", eaDefSearchCalls)
	}
}

func TestClusterDeleteSurfacesPrerequisiteErrorFromIdentitySearch(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = `{"Error":"AdmConAuthError: Not authorized"}`
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-delete-undefined-ea"}
	cr := newClusterSRVRecord("my-srvrecord", "")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Delete: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
}

func TestClusterDeleteSteadyStateNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-delete-steady-state"}
	cr := newClusterSRVRecord("my-srvrecord", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error on a reference that resolves directly: %v", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — the steady-state delete path must never probe", eaDefSearchCalls)
	}
}

func TestNamespacedObserveSurfacesPrerequisiteErrorFromIdentitySearch(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = `{"Error":"AdmConAuthError: Not authorized"}`
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ns-observe-undefined-ea"}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "", "ProviderConfig")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Observe: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
}

func TestNamespacedObserveSteadyStateNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ns-steady-state"}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", ref, "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error on a reference that resolves directly: %v", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — the steady-state (reference resolves) path must never probe", eaDefSearchCalls)
	}
}

func TestNamespacedDeleteSurfacesPrerequisiteErrorFromIdentitySearch(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = `{"Error":"AdmConAuthError: Not authorized"}`
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ns-delete-undefined-ea"}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", "", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Delete: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
}

func TestNamespacedDeleteSteadyStateNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordSRV{
		Name:     stringPtr("_sip._tcp.example.com"),
		Target:   stringPtr("sipserver.example.com"),
		Priority: uint32Ptr(10),
		Weight:   uint32Ptr(20),
		Port:     uint32Ptr(5060),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ns-delete-steady-state"}
	cr := newNamespacedSRVRecord("default", "my-srvrecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error on a reference that resolves directly: %v", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — the steady-state delete path must never probe", eaDefSearchCalls)
	}
}
