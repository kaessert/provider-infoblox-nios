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
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	cperrors "github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordmx/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordmx/v1alpha1"
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

// errBodyEADefUnprivileged is the WAPI response body a Grid returns when
// the configured credential lacks the superuser privilege required to
// create the identity extensible attribute definition — used by the
// prerequisite-probe refusal tests.
const errBodyEADefUnprivileged = `{"Error":"AdmConAuthError: Not authorized"}`

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
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.MXRecordSpec{
			ClusterManagedResourceSpec: xpv2.ClusterManagedResourceSpec{
				ProviderConfigReference: &xpv2.Reference{Name: "default"},
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
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
		Spec: namespacedv1alpha1.MXRecordSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: pcKind, Name: "default"},
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

	// searchCalls counts requests to the search endpoint (a GET with no
	// _ref path segment) — used to prove the identity ladder actually
	// issued a round trip rather than short-circuiting.
	searchCalls int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests inspecting exactly what the controller sends.
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

	// createCalls/putCalls count POST/PUT requests against record:mx
	// itself (independent of eaDefCreateCalls above), used to prove a
	// Create call issues exactly one mutating request — no follow-up PUT
	// to re-assert the identity stamp — and that a refused Create/Update
	// issues zero of either.
	createCalls int
	putCalls    int
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{
		records: map[string]*ibclient.RecordMX{},
		// The identity EA definition is present by default so every
		// pre-existing Create test sees the prerequisite as already
		// satisfied and never exercises the create-definition path.
		eaDefExists: true,
	}
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

	// Search endpoint (GetMXRecord): a GET with no _ref path segment,
	// filtered by view/name/mail_exchanger/preference query params.
	// Registered as an exact literal path so Go's ServeMux prefers it
	// over the {ref...} wildcard below for requests to precisely
	// "record:mx" (real _refs always carry additional path segments).
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

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/record:mx", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.searchCalls++
		m.mu.Unlock()

		q := r.URL.Query()
		view := q.Get("view")
		name := q.Get("name")
		mx := q.Get("mail_exchanger")
		pref := q.Get("preference")

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
		var incoming ibclient.RecordMX
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		m.putCalls++

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
	mux := http.NewServeMux()
	// The identity-prerequisite probe (see ensureIdentityPrerequisite) issues
	// its own separate request. Serving it a positive verdict here keeps a
	// "boom" mock scoped to the operation it exists to exercise (Create,
	// Update, or a search), instead of the probe itself absorbing the
	// injected failure and masking the assertion under test.
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, _ *http.Request) {
		name := identity.EAKey
		writeJSON(w, http.StatusOK, []ibclient.EADefinition{{Name: &name}})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"Error":"boom"}`))
	})
	return mux
}

// newTestObjectManager builds an ibclient.IBObjectManager pointed at the
// given httptest.Server via plain HTTP (no TLS needed — the WapiRequestBuilder
// only switches to HTTPS when hostCfg.Scheme != "http").
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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Comment:       stringPtr("hello"),
		Ttl:           func() *uint32 { v := uint32(300); return &v }(),
		UseTtl:        boolPtr(true),
		Ea:            identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", ref)
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
	if cr.Status.AtProvider.Zone == nil || *cr.Status.AtProvider.Zone != "com" {
		t.Errorf("AtProvider.Zone = %v, want com", cr.Status.AtProvider.Zone)
	}
	if cr.Status.AtProvider.Preference == nil || *cr.Status.AtProvider.Preference != 10 {
		t.Errorf("AtProvider.Preference = %v, want 10", cr.Status.AtProvider.Preference)
	}
	if cond := cr.GetCondition(xpv2.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/does-not-exist:example.com/default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404 with no matching search result, got true")
	}
}

// TestClusterObserveRefInstabilityFallsBackToSearch verifies that a stale
// _ref (as if the record was renamed by a prior reconcile that changed
// identity fields, but the annotation refresh was lost — e.g. controller
// restart) is recovered through the identity-EA search rather than
// permanently reporting the resource as deleted.
func TestClusterObserveRefInstabilityFallsBackToSearch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	realRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", "") // external-name unset
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/test1:example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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
		Ea:            identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

// TestClusterUpdatePrerequisiteAutoCreates verifies the
// unconditional Update guard: when the identity extensible attribute
// definition is absent but the configured credential can create one, the
// probe auto-creates it before the mutating PUT, and the update proceeds
// normally — this is the exact path a pre-existing, unstamped object hits
// on every reconcile (Observe resolves it as OutcomeAdopted, forcing
// Update), so the auto-create must be reachable from here, not just from
// Create.
func TestClusterUpdatePrerequisiteAutoCreates(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Comment:       stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	defExists := m.eaDefExists
	createCalls := m.eaDefCreateCalls
	m.mu.Unlock()
	if !defExists {
		t.Error("Update: eaDefExists = false, want true — the prerequisite probe must auto-create the identity definition before the mutating call")
	}
	if createCalls != 1 {
		t.Errorf("Update: eaDefCreateCalls = %d, want exactly 1", createCalls)
	}
}

// TestClusterUpdatePrerequisiteRefusesUncreatable verifies the
// unconditional Update guard on the refusal side: when the identity
// extensible attribute definition is absent and the configured credential
// cannot create one, Update returns the typed PrerequisiteError (not a raw
// wrapped WAPI 400) and issues no mutating call — the object is left
// exactly as it was.
func TestClusterUpdatePrerequisiteRefusesUncreatable(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = errBodyEADefUnprivileged
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Comment:       stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Update: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}

	m.mu.Lock()
	defExists := m.eaDefExists
	m.mu.Unlock()
	if defExists {
		t.Error("Update: eaDefExists = true, want false — a refused create must not be treated as success")
	}
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name = %q, want unchanged %q — a refused prerequisite must issue no mutating call", got, ref)
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

// TestClusterUpdateError verifies that a 5xx response from the WAPI is
// propagated (wrapped, not swallowed). A totally unresponsive Grid fails
// at the identity-prerequisite probe stage (issued unconditionally before
// the update call itself, mirroring Create).
func TestClusterUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	ref := m.seed(&ibclient.RecordMX{Name: stringPtr("example.com"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/test1:example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteMXRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteMXRecord)
	}
}

// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject verifies the
// core defect fix: a 404 against the stored _ref must not be treated as
// "already deleted" when a natural-key search finds the same identity
// still live under a different _ref (the _ref merely rotated because an
// identity field changed and the refreshed annotation was never
// persisted). Deleting that record would be unverifiable ownership, so
// Delete() must refuse and leave the record in place.
func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// A live record sharing newClusterMXRecord's identity fields, but
	// filed under a _ref that does not match the CR's stored
	// external-name.
	liveRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		View:          stringPtr("default"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		Ea:            identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/stale-ref:example.com/default")

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

	foreignRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", foreignRef)

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
	cr := newClusterMXRecord("my-mxrecord", "record:mx/stale-ref:example.com/default")

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

	foreignRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, "someone-elses-uid"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", foreignRef)

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
	e := &clusterExternal{kube: &recordingKubeClient{}, prober: identity.NewProber(), endpoint: t.Name()}
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
						Source: xpv2.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
							SecretRef: &xpv2.SecretKeySelector{
								SecretReference: xpv2.SecretReference{Name: secret, Namespace: ns},
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "", "ProviderConfig")
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
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/test1:example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

// TestNamespacedUpdatePrerequisiteAutoCreates verifies the
// unconditional Update guard: when the identity extensible attribute
// definition is absent but the configured credential can create one, the
// probe auto-creates it before the mutating PUT, and the update proceeds
// normally — this is the exact path a pre-existing, unstamped object hits
// on every reconcile (Observe resolves it as OutcomeAdopted, forcing
// Update), so the auto-create must be reachable from here, not just from
// Create.
func TestNamespacedUpdatePrerequisiteAutoCreates(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.MailExchanger = stringPtr("mail2.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	defExists := m.eaDefExists
	createCalls := m.eaDefCreateCalls
	m.mu.Unlock()
	if !defExists {
		t.Error("Update: eaDefExists = false, want true — the prerequisite probe must auto-create the identity definition before the mutating call")
	}
	if createCalls != 1 {
		t.Errorf("Update: eaDefCreateCalls = %d, want exactly 1", createCalls)
	}
}

// TestNamespacedUpdatePrerequisiteRefusesUncreatable verifies the
// unconditional Update guard on the refusal side: when the identity
// extensible attribute definition is absent and the configured credential
// cannot create one, Update returns the typed PrerequisiteError (not a raw
// wrapped WAPI 400) and issues no mutating call — the object is left
// exactly as it was.
func TestNamespacedUpdatePrerequisiteRefusesUncreatable(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = errBodyEADefUnprivileged
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.MailExchanger = stringPtr("mail2.example.com")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Update: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}

	m.mu.Lock()
	defExists := m.eaDefExists
	m.mu.Unlock()
	if defExists {
		t.Error("Update: eaDefExists = true, want false — a refused create must not be treated as success")
	}
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name = %q, want unchanged %q — a refused prerequisite must issue no mutating call", got, ref)
	}
}

// TestNamespacedUpdateError verifies that a 5xx response from the WAPI is
// propagated (wrapped, not swallowed). A totally unresponsive Grid fails
// at the identity-prerequisite probe stage (issued unconditionally before
// the update call itself, mirroring Create).
func TestNamespacedUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	ref := m.seed(&ibclient.RecordMX{Name: stringPtr("example.com"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/test1:example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteMXRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteMXRecord)
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		View:          stringPtr("default"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		Ea:            identity.Stamp(nil, testUIDNamespaced),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/stale-ref:example.com/default", "ProviderConfig")

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

	foreignRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", foreignRef, "ProviderConfig")

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
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/stale-ref:example.com/default", "ProviderConfig")

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

	foreignRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, "someone-elses-uid"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", foreignRef, "ProviderConfig")

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
						Source: xpv2.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
							SecretRef: &xpv2.SecretKeySelector{
								SecretReference: xpv2.SecretReference{Name: secret, Namespace: ns},
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
						Source: xpv2.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
							SecretRef: &xpv2.SecretKeySelector{
								SecretReference: xpv2.SecretReference{Name: secret, Namespace: ns},
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
	e := &namespacedExternal{kube: &recordingKubeClient{}, prober: identity.NewProber(), endpoint: t.Name()}
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
	ttl := uint32Ptr(120)
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
	var ttl *uint32
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
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
		ttl           *uint32
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
			ttl:           uint32Ptr(300),
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
			ttl:           uint32Ptr(300),
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
			ttl:           uint32Ptr(300),
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
			ttl:           uint32Ptr(300),
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
			ttl:           uint32Ptr(300),
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
			ttl:           uint32Ptr(600),
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
			ttl:           uint32Ptr(300),
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
			ttl:           uint32Ptr(300),
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
			ttl:           uint32Ptr(300),
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
		uint32Ptr(0),
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
		uint32Ptr(300),
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

func TestUint32PtrOrZero(t *testing.T) {
	cases := map[string]struct {
		reason string
		v      *uint32
		want   uint32
	}{
		"NilReturnsZero": {
			reason: "an unset pointer must map to 0 — the WAPI create/update calls take a plain uint32 with no separate unset sentinel",
			v:      nil,
			want:   0,
		},
		"ValidValuePassesThrough": {
			reason: "a set value must pass through unchanged",
			v:      uint32Ptr(300),
			want:   300,
		},
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

	creds, err := extractCredentials(context.Background(), kube, xpv2.CredentialsSourceSecret, &xpv2.SecretKeySelector{
		SecretReference: xpv2.SecretReference{Name: "infobloxnios-credentials", Namespace: "crossplane-system"},
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

// ── Identity: stamp isolation from spec.forProvider ─────────────────────

func TestLateInitializeStripsIdentityEAFromExtAttrs(t *testing.T) {
	var comment *string
	var ttl *uint32
	var useTTL *bool
	extAttrs := map[string]string(nil)

	rec := &ibclient.RecordMX{
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

func TestIsUpToDateIgnoresIdentityEA(t *testing.T) {
	rec := &ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		Ea:            identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	}

	got := isUpToDate(stringPtr("example.com"), stringPtr("mail.example.com"), int64Ptr(10), nil, nil, nil, map[string]string{"env": "prod"}, rec)
	if !got {
		t.Error("isUpToDate: want true when spec.forProvider.extAttrs matches the Grid map with the identity stamp stripped, got false")
	}
}

func TestClusterObserveAtProviderExtAttrsIncludesIdentityKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", ref)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; got != testUIDCluster {
		t.Errorf("AtProvider.ExtAttrs[%q] = %q, want %q (full Grid EA mirror, stamp included)", identity.EAKey, got, testUIDCluster)
	}
}

// ── Identity: empty-uid refusal ──────────────────────────────────────────

func TestCreateMXRecordRefusesEmptyUID(t *testing.T) {
	_, err := createMXRecord(nil, stringPtr("default"), stringPtr("example.com"), stringPtr("mail.example.com"), int64Ptr(10), nil, nil, nil, nil, "")
	if err == nil {
		t.Fatal("createMXRecord: expected an error for an empty uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("createMXRecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateMXRecordRefusesEmptyUID(t *testing.T) {
	_, err := updateMXRecord(nil, "record:mx/test1:example.com/default", stringPtr("default"), stringPtr("example.com"), stringPtr("mail.example.com"), int64Ptr(10), nil, nil, nil, nil, "")
	if err == nil {
		t.Fatal("updateMXRecord: expected an error for an empty uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("updateMXRecord: error = %v, want it to mention the empty uid", err)
	}
}

// TestCreateMXRecordRefusesWhitespaceUID and
// TestUpdateMXRecordRefusesWhitespaceUID: a whitespace-only uid is not
// empty by a literal "" comparison, but it is not a usable identity
// either — the guard must trim before checking, matching the shared
// identity resolution ladder's own TrimSpace check.

func TestCreateMXRecordRefusesWhitespaceUID(t *testing.T) {
	_, err := createMXRecord(nil, stringPtr("default"), stringPtr("example.com"), stringPtr("mail.example.com"), int64Ptr(10), nil, nil, nil, nil, "   ")
	if err == nil {
		t.Fatal("createMXRecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("createMXRecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateMXRecordRefusesWhitespaceUID(t *testing.T) {
	_, err := updateMXRecord(nil, "record:mx/test1:example.com/default", stringPtr("default"), stringPtr("example.com"), stringPtr("mail.example.com"), int64Ptr(10), nil, nil, nil, nil, "   ")
	if err == nil {
		t.Fatal("updateMXRecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("updateMXRecord: error = %v, want it to mention the empty uid", err)
	}
}

// ── identity ladder: every row, both scopes ─────────────────────────────
//
// What follows fills the remaining rows the pilot (recorda) covers:
// Rotated, Adopted, FoundByUID, AmbiguousMatchError, and the namespaced
// HandleReuseError row — so no ladder outcome is exercised on only one
// scope.

func TestClusterObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/stale-ref:example.com/default")

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

	newRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/stale-ref:example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		// No Ea at all — the object has never been stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", ref)

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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		// No Ea at all — the object has never been stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")

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

	foundRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", "")
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

	foundRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "", "ProviderConfig")
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

	m.seed(&ibclient.RecordMX{Name: stringPtr("host-a.example.com"), MailExchanger: stringPtr("mail-a.example.com"), Preference: uint32Ptr(10), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.RecordMX{Name: stringPtr("host-b.example.com"), MailExchanger: stringPtr("mail-b.example.com"), Preference: uint32Ptr(20), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/stale-ref:example.com/default")

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

	m.seed(&ibclient.RecordMX{Name: stringPtr("host-a.example.com"), MailExchanger: stringPtr("mail-a.example.com"), Preference: uint32Ptr(10), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})
	m.seed(&ibclient.RecordMX{Name: stringPtr("host-b.example.com"), MailExchanger: stringPtr("mail-b.example.com"), Preference: uint32Ptr(20), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/stale-ref:example.com/default", "ProviderConfig")

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

	refA := m.seed(&ibclient.RecordMX{Name: stringPtr("host-a.example.com"), MailExchanger: stringPtr("mail-a.example.com"), Preference: uint32Ptr(10), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})
	refB := m.seed(&ibclient.RecordMX{Name: stringPtr("host-b.example.com"), MailExchanger: stringPtr("mail-b.example.com"), Preference: uint32Ptr(20), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/stale-ref:example.com/default")

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

	refA := m.seed(&ibclient.RecordMX{Name: stringPtr("host-a.example.com"), MailExchanger: stringPtr("mail-a.example.com"), Preference: uint32Ptr(10), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})
	refB := m.seed(&ibclient.RecordMX{Name: stringPtr("host-b.example.com"), MailExchanger: stringPtr("mail-b.example.com"), Preference: uint32Ptr(20), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "record:mx/stale-ref:example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		// No Ea at all — never stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", ref)

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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		// No Ea at all — never stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")

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
	cr := newClusterMXRecord("my-mxrecord", "")

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
		t.Errorf("Create: POST /record:mx calls = %d, want exactly 1", createCalls)
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
	cr := newNamespacedMXRecord("default", "my-mxrecord", "", "ProviderConfig")

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
		t.Errorf("Create: POST /record:mx calls = %d, want exactly 1", createCalls)
	}
	if putCalls != 0 {
		t.Errorf("Create: PUT calls = %d, want 0 — the identity stamp must land in the same request that creates the object, no follow-up PUT", putCalls)
	}
}

// TestCreateMXRecordRefusesEmptyUIDIssuesNoMutatingCall is the
// controller-level (not just the bare-function) companion of
// TestCreateMXRecordRefusesEmptyUID: proves the httptest server records
// zero mutating requests when Create is refused for an empty uid.
func TestCreateMXRecordRefusesEmptyUIDIssuesNoMutatingCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", "")
	cr.SetUID("")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected an error for a blank metadata.uid, got nil")
	}

	m.mu.Lock()
	createCalls, eaDefCreateCalls := m.createCalls, m.eaDefCreateCalls
	m.mu.Unlock()
	if createCalls != 0 {
		t.Errorf("Create: POST /record:mx calls = %d, want 0 for a refused create", createCalls)
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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterMXRecord("my-mxrecord", ref)
	// Change only extAttrs — name/mail_exchanger/preference changes
	// rotate the _ref for this resource (see the PUT handler's
	// identityChanged simulation), which is already covered elsewhere.
	// This test isolates the identity-reassert property from that
	// rotation.
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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(ibclient.EA{"env": "prod"}, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")
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

	oldRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
	})

	cr := newClusterMXRecord("my-mxrecord", oldRef)
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating rename, want a refreshed _ref")
	}

	fetched := &clusterv1alpha1.MXRecord{}
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

	oldRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
	})

	cr := newNamespacedMXRecord("default", "my-mxrecord", oldRef, "ProviderConfig")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating rename, want a refreshed _ref")
	}

	fetched := &namespacedv1alpha1.MXRecord{}
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
	m.eaDefCreateBody = errBodyEADefUnprivileged
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-observe-undefined-ea"}
	// No external-name ever assigned: observeRefFor reports "" for this
	// case, sending the ladder straight to the identity-EA search with
	// no ref-GET attempt first.
	cr := newClusterMXRecord("my-mxrecord", "")

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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-steady-state"}
	cr := newClusterMXRecord("my-mxrecord", ref)

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

	foreignRef := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-foreign-identity"}
	cr := newClusterMXRecord("my-mxrecord", foreignRef)

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
	cr := newClusterMXRecord("my-mxrecord", "record:mx/stale-ref:example.com/default")

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

	m.seed(&ibclient.RecordMX{Name: stringPtr("host-a.example.com"), MailExchanger: stringPtr("mail-a.example.com"), Preference: uint32Ptr(10), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.RecordMX{Name: stringPtr("host-b.example.com"), MailExchanger: stringPtr("mail-b.example.com"), Preference: uint32Ptr(20), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ambiguous"}
	cr := newClusterMXRecord("my-mxrecord", "record:mx/stale-ref:example.com/default")

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
	m.eaDefCreateBody = errBodyEADefUnprivileged
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-delete-undefined-ea"}
	cr := newClusterMXRecord("my-mxrecord", "")

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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-delete-steady-state"}
	cr := newClusterMXRecord("my-mxrecord", ref)

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
	m.eaDefCreateBody = errBodyEADefUnprivileged
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ns-observe-undefined-ea"}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ns-steady-state"}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")

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
	m.eaDefCreateBody = errBodyEADefUnprivileged
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ns-delete-undefined-ea"}
	cr := newNamespacedMXRecord("default", "my-mxrecord", "", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordMX{
		Name:          stringPtr("example.com"),
		MailExchanger: stringPtr("mail.example.com"),
		Preference:    uint32Ptr(10),
		View:          stringPtr("default"),
		Ea:            identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ns-delete-steady-state"}
	cr := newNamespacedMXRecord("default", "my-mxrecord", ref, "ProviderConfig")

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
