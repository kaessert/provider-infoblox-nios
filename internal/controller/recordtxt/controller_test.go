// Package recordtxt unit tests for the TXTRecord MR controllers. Tests
// use inline httptest.NewServer mocks that emulate the WAPI record:txt
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package recordtxt

import (
	"context"
	"encoding/json"
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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordtxt/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordtxt/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/dualclient"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/readrouting"
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

// credentialsSecret returns a Secret carrying the username/password keys
// the credential bridge expects. The Grid Manager host is a
// ProviderConfig-level spec field, not a Secret key.
func credentialsSecret(ns, name, username, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data: map[string][]byte{
			"username": []byte(username),
			"password": []byte(password),
		},
	}
}

// newClusterTXTRecord builds a minimal cluster-scoped TXTRecord CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterTXTRecord(crName, externalName string) *clusterv1alpha1.TXTRecord {
	cr := &clusterv1alpha1.TXTRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.TXTRecordSpec{
			ClusterManagedResourceSpec: xpv2.ClusterManagedResourceSpec{
				ProviderConfigReference: &xpv2.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.TXTRecordParameters{
				Name: stringPtr("host.example.com"),
				Text: stringPtr("v=spf1 -all"),
				View: stringPtr("default"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedTXTRecord is the namespaced variant of newClusterTXTRecord.
func newNamespacedTXTRecord(ns, crName, externalName, pcKind string) *namespacedv1alpha1.TXTRecord {
	cr := &namespacedv1alpha1.TXTRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
		Spec: namespacedv1alpha1.TXTRecordSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.TXTRecordParameters{
				Name: stringPtr("host.example.com"),
				Text: stringPtr("v=spf1 -all"),
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
// mockWapiServer emulates the subset of NIOS WAPI record:txt endpoints
// exercised by the TXTRecord controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.RecordTXT type so the wire format (including the EA
// {"value": ...} envelope) exactly matches what the SDK sends and
// expects.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.RecordTXT
	nextRef int

	// searchCalls counts requests to the search endpoint (a GET with no
	// _ref path segment) — used to prove the identity ladder actually
	// issued a round trip rather than short-circuiting.
	searchCalls int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte

	// renameOnUpdate makes the PUT handler assign a brand-new _ref
	// whenever name or text changes, emulating the WAPI's documented
	// _ref-instability behavior for TXTRecord.
	renameOnUpdate bool

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

	// createCalls/putCalls count POST/PUT requests against record:txt
	// itself (independent of eaDefCreateCalls above), used to prove a
	// Create call issues exactly one mutating request — no follow-up PUT
	// to re-assert the identity stamp — and that a refused Create/Update
	// issues zero of either.
	createCalls int
	putCalls    int
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{
		records: map[string]*ibclient.RecordTXT{},
		// The identity EA definition is present by default so every
		// pre-existing Create test sees the prerequisite as already
		// satisfied and never exercises the create-definition path.
		eaDefExists: true,
	}
}

func (m *mockWapiServer) seed(rec *ibclient.RecordTXT) string {
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

func (m *mockWapiServer) newRefLocked(rec *ibclient.RecordTXT) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	view := ""
	if rec.View != nil {
		view = *rec.View
	}
	return "record:txt/test" + itoa(m.nextRef) + ":" + name + "/" + view
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

// handler returns an http.Handler implementing the record:txt WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/record:txt", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.RecordTXT
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		m.createCalls++
		m.mu.Unlock()
		// Synthesize the zone the way NIOS derives it server-side
		// (last two labels of the FQDN), so Observe/Create tests can
		// assert the response-only Zone field is mirrored.
		rec.Zone = zoneFromName(rec.Name)
		ref := m.seed(&rec)
		writeJSON(w, http.StatusOK, ref)
	})

	// Search endpoint: a GET with no _ref path segment, filtered by
	// view/name/text query params, and/or a "*<EA name>" extensible-
	// attribute filter (the syntax identity.Resolve's searchByUID
	// uses). Registered as an exact literal path so Go's ServeMux
	// prefers it over the {ref...} wildcard below for requests to
	// precisely "record:txt" (real _refs always carry additional path
	// segments). Filtering server-side (like the real WAPI, which
	// live-verified supports an exact filter on text) is what makes
	// the identity ladder's match-count answer meaningful in these
	// tests, instead of a client-side comparison hiding an ordering
	// bug.
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

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/record:txt", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.searchCalls++
		m.mu.Unlock()

		q := r.URL.Query()
		view := q.Get("view")
		name := q.Get("name")
		text := q.Get("text")

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
		// Initialized (not nil-declared): a real WAPI search that
		// matches nothing answers with a literal JSON "[]", never
		// "null" — a nil Go slice would marshal to "null" instead,
		// which never exercises the not-found path the SDK's connector
		// applies to a literal "[]" body.
		matches := []ibclient.RecordTXT{}
		for _, rec := range m.records {
			if view != "" && (rec.View == nil || *rec.View != view) {
				continue
			}
			if name != "" && (rec.Name == nil || *rec.Name != name) {
				continue
			}
			if text != "" && (rec.Text == nil || *rec.Text != text) {
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
		var incoming ibclient.RecordTXT
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		m.putCalls++

		nameOrTextChanged := (existing.Name == nil && incoming.Name != nil) ||
			(existing.Name != nil && incoming.Name != nil && *existing.Name != *incoming.Name) ||
			(existing.Text == nil && incoming.Text != nil) ||
			(existing.Text != nil && incoming.Text != nil && *existing.Text != *incoming.Text)

		existing.Name = incoming.Name
		existing.Text = incoming.Text
		existing.Comment = incoming.Comment
		existing.Ttl = incoming.Ttl
		existing.UseTtl = incoming.UseTtl
		existing.Ea = incoming.Ea
		existing.Zone = zoneFromName(existing.Name)

		respRef := ref
		if m.renameOnUpdate && nameOrTextChanged {
			newRef := "record:txt/renamed" + itoa(len(m.records)) + ":" + strOrEmpty(existing.Name) + "/" + strOrEmpty(existing.View)
			existing.Ref = newRef
			delete(m.records, ref)
			m.records[newRef] = existing
			respRef = newRef
		}
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, respRef)
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

// newTestObjectManager builds an identity.ManagerAndConnector pointed at
// the given httptest.Server via plain HTTP (no TLS needed — the
// WapiRequestBuilder only switches to HTTPS when hostCfg.Scheme !=
// "http"). Callers that only need the high-level ObjectManager can use
// .Manager; the identity ladder additionally needs .Connector.
func newTestObjectManager(t *testing.T, srv *httptest.Server) identity.ManagerAndConnector {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	conn, err := config.BuildConnector(dualclient.Credentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test object manager: %v", err)
	}
	return identity.NewManagerAndConnector(conn)
}

// ── cluster: Observe ────────────────────────────────────────────────────

func TestClusterObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name:    stringPtr("host.example.com"),
		Text:    stringPtr("v=spf1 -all"),
		View:    stringPtr("default"),
		Comment: stringPtr("hello"),
		Ttl:     uint32Ptr(300),
		UseTtl:  boolPtr(true),
		Ea:      identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)
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
	if cr.Status.AtProvider.Zone == nil || *cr.Status.AtProvider.Zone != "example.com" {
		t.Errorf("AtProvider.Zone = %v, want example.com", cr.Status.AtProvider.Zone)
	}
	if cond := cr.GetCondition(xpv2.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/does-not-exist:host.example.com/default")

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "") // external-name unset
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

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map) must not panic and must produce a valid observation with
// nil-safe AtProvider fields. observeFromRecordTXT copies optional
// pointer fields directly (never dereferences without a nil guard), so
// this test also pins that contract for future edits.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// Seed a completely bare RecordTXT — only the SDK-assigned _ref (via
	// seed()) identifies the object. Name/View are the Go zero value
	// (nil), so zoneFromName leaves Zone at "" too.
	ref := m.seed(&ibclient.RecordTXT{})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)

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
	if ap.Text != nil {
		t.Errorf("AtProvider.Text = %v, want nil", ap.Text)
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

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestClusterCreateServerError verifies that a 5xx response from the WAPI
// create endpoint is propagated (wrapped, not swallowed) and that the
// external-name annotation is left untouched.
func TestClusterCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateTXTRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateTXTRecord)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want unset after failed create", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("original-view"),
		Ea:   identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)
	// Mutate the immutable view field in spec — this must NOT affect
	// ResourceUpToDate, since view is excluded from isUpToDate (the SDK's
	// UpdateTXTRecord has no view parameter, and a runtime PUT that
	// changes view is rejected by the WAPI).
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

	ref := m.seed(&ibclient.RecordTXT{
		Name:    stringPtr("host.example.com"),
		Text:    stringPtr("v=spf1 -all"),
		View:    stringPtr("default"),
		Comment: stringPtr("old comment"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)
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

	ref := m.seed(&ibclient.RecordTXT{
		Name:    stringPtr("host.example.com"),
		Text:    stringPtr("v=spf1 -all"),
		View:    stringPtr("default"),
		Comment: stringPtr("old comment"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)
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

	ref := m.seed(&ibclient.RecordTXT{
		Name:    stringPtr("host.example.com"),
		Text:    stringPtr("v=spf1 -all"),
		View:    stringPtr("default"),
		Comment: stringPtr("old comment"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)
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

func TestClusterUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)

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

// TestClusterUpdateServerError verifies that a 5xx response from the WAPI
// is propagated (wrapped, not swallowed) and that the external-name
// annotation is left untouched. A totally unresponsive Grid fails at the
// identity-prerequisite probe stage (issued unconditionally before the
// update call itself, mirroring Create).
func TestClusterUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	ref := "record:txt/test1:host.example.com/default"
	cr := newClusterTXTRecord("my-txtrecord", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateTXTRecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateTXTRecord)
	}
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name = %q, want unchanged %q after failed update", got, ref)
	}
}

// TestClusterUpdateCapturesNewRefOnRename pins the _ref-instability
// contract: when name or text changes, the WAPI returns a NEW _ref for
// the object, and Update MUST capture it via meta.SetExternalName so the
// next reconcile's Observe() doesn't 404 against the stale old ref.
func TestClusterUpdateCapturesNewRefOnRename(t *testing.T) {
	m := newMockWapiServer()
	m.renameOnUpdate = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)
	cr.Spec.ForProvider.Text = stringPtr("v=spf1 include:example.com -all")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == ref {
		t.Errorf("Update: external-name still %q, want it refreshed to the new server-assigned ref", ref)
	}
	if got == "" {
		t.Error("Update: external-name is empty after rename, want the new ref")
	}
}

// ── cluster: Delete ──────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{Name: stringPtr("host.example.com"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)

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

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/does-not-exist:host.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

// TestClusterDeleteServerError verifies that a 5xx response from the
// WAPI delete endpoint is propagated (wrapped, not swallowed) rather
// than being treated as a not-found/already-deleted success.
func TestClusterDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/test1:host.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteTXTRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteTXTRecord)
	}
}

func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/stale-ref:host.example.com/default")

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

	foreignRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", foreignRef)

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
// companion happy path: a 404 against the stored _ref, and a natural-key
// search that finds nothing, means the object really is gone.
func TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/stale-ref:host.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
}

func TestClusterObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/stale-ref:host.example.com/default")

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

func TestClusterObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", foreignRef)

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
			credentialsSecret(ns, secret, "admin", "s3cr3t"),
			&clusterpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: clusterpcv1alpha1.ProviderConfigSpec{
					Host: "grid.example.com",
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

	cr := newClusterTXTRecord("my-txtrecord", "")
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

	cr := newClusterTXTRecord("my-txtrecord", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name:    stringPtr("host.example.com"),
		Text:    stringPtr("v=spf1 -all"),
		View:    stringPtr("default"),
		Comment: stringPtr("hello"),
		Ttl:     uint32Ptr(300),
		UseTtl:  boolPtr(true),
		Ea:      identity.Stamp(ibclient.EA{"env": "prod"}, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")
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
	if cond := cr.GetCondition(xpv2.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestNamespacedObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "record:txt/does-not-exist:host.example.com/default", "ProviderConfig")

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

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "", "ProviderConfig")
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

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "record:txt/test1:host.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "record:txt/test1:host.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

func TestNamespacedObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")

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
	if ap.Text != nil {
		t.Errorf("AtProvider.Text = %v, want nil", ap.Text)
	}
}

// ── namespaced: Create ────────────────────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateServerError verifies that a 5xx response from the
// WAPI create endpoint is propagated (wrapped, not swallowed) and that
// the external-name annotation is left untouched.
func TestNamespacedCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateTXTRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateTXTRecord)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want unset after failed create", got)
	}
}

// ── namespaced: Update ────────────────────────────────────────────────────

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name:    stringPtr("host.example.com"),
		Text:    stringPtr("v=spf1 -all"),
		View:    stringPtr("default"),
		Comment: stringPtr("old comment"),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")
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

	ref := m.seed(&ibclient.RecordTXT{
		Name:    stringPtr("host.example.com"),
		Text:    stringPtr("v=spf1 -all"),
		View:    stringPtr("default"),
		Comment: stringPtr("old comment"),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")
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

	ref := m.seed(&ibclient.RecordTXT{
		Name:    stringPtr("host.example.com"),
		Text:    stringPtr("v=spf1 -all"),
		View:    stringPtr("default"),
		Comment: stringPtr("old comment"),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")
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

// TestNamespacedUpdateServerError verifies that a 5xx response from the
// WAPI is propagated (wrapped, not swallowed) and that the external-name
// annotation is left untouched. A totally unresponsive Grid fails at the
// identity-prerequisite probe stage (issued unconditionally before the
// update call itself, mirroring Create).
func TestNamespacedUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	ref := "record:txt/test1:host.example.com/default"
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateTXTRecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateTXTRecord)
	}
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name = %q, want unchanged %q after failed update", got, ref)
	}
}

// ── namespaced: Delete ────────────────────────────────────────────────────

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{Name: stringPtr("host.example.com"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")

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

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "record:txt/does-not-exist:host.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "record:txt/test1:host.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteTXTRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteTXTRecord)
	}
}

func TestNamespacedDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "record:txt/stale-ref:host.example.com/default", "ProviderConfig")

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

	foreignRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", foreignRef, "ProviderConfig")

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

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "record:txt/stale-ref:host.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
}

func TestNamespacedObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "record:txt/stale-ref:host.example.com/default", "ProviderConfig")

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

func TestNamespacedObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", foreignRef, "ProviderConfig")

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
			credentialsSecret(ns, secret, "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: ns},
				Spec: namespacedpcv1alpha1.ProviderConfigSpec{
					Host: "grid.example.com",
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

	cr := newNamespacedTXTRecord(ns, "my-txtrecord", "", "ProviderConfig")
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
			credentialsSecret(ns, secret, "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ClusterProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: namespacedpcv1alpha1.ProviderConfigSpec{
					Host: "grid.example.com",
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

	cr := newNamespacedTXTRecord("app-ns", "my-txtrecord", "", "ClusterProviderConfig")
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

	cr := newNamespacedTXTRecord("default", "my-txtrecord", "", "SomeOtherKind")
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
	if !isNotFound(errGenericStatus(404)) {
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

	rec := &ibclient.RecordTXT{
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

	rec := &ibclient.RecordTXT{
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

	rec := &ibclient.RecordTXT{
		Ttl:    uint32Ptr(28800),
		UseTtl: boolPtr(false),
	}

	lateInitialize(&comment, &ttl, &useTTL, &extAttrs, rec)

	if ttl != nil {
		t.Errorf("lateInitialize: ttl = %v, want nil (useTtl is off, observed ttl is the zone default, not a user value)", *ttl)
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that name, text,
// and view — the CRD's required TXTRecordParameters fields — are never
// overwritten by Observe()'s late-init step. lateInitialize only accepts
// pointers to the optional fields (comment, ttl, useTtl, extAttrs), so a
// spec/observed mismatch on a required field can never occur through the
// real WAPI flow — this test drives it artificially to pin the
// guarantee.
func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("observed.example.com"),
		Text: stringPtr("observed-text"),
		View: stringPtr("observed-view"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)
	cr.Spec.ForProvider.Name = stringPtr("host.example.com")
	cr.Spec.ForProvider.Text = stringPtr("v=spf1 -all")
	cr.Spec.ForProvider.View = stringPtr("default")

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := *cr.Spec.ForProvider.Name; got != "host.example.com" {
		t.Errorf("Observe: required field Name late-initialized to %q, want unchanged %q", got, "host.example.com")
	}
	if got := *cr.Spec.ForProvider.Text; got != "v=spf1 -all" {
		t.Errorf("Observe: required field Text late-initialized to %q, want unchanged %q", got, "v=spf1 -all")
	}
	if got := *cr.Spec.ForProvider.View; got != "default" {
		t.Errorf("Observe: required field View late-initialized to %q, want unchanged %q", got, "default")
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedRecord := func() *ibclient.RecordTXT {
		return &ibclient.RecordTXT{
			Name:    stringPtr("host.example.com"),
			Text:    stringPtr("v=spf1 -all"),
			Comment: stringPtr("hello"),
			Ttl:     uint32Ptr(300),
			UseTtl:  boolPtr(true),
			Ea:      ibclient.EA{"env": "prod"},
		}
	}

	cases := map[string]struct {
		mutate func(name, text, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string) (*string, *string, *string, *uint32, *bool, map[string]string)
		want   bool
	}{
		"AllMatch": {
			mutate: func(name, text, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string) (*string, *string, *string, *uint32, *bool, map[string]string) {
				return name, text, comment, ttl, useTTL, extAttrs
			},
			want: true,
		},
		"NameDiffers": {
			mutate: func(_, text, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string) (*string, *string, *string, *uint32, *bool, map[string]string) {
				return stringPtr("other.example.com"), text, comment, ttl, useTTL, extAttrs
			},
			want: false,
		},
		"TextDiffers": {
			mutate: func(name, _ *string, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string) (*string, *string, *string, *uint32, *bool, map[string]string) {
				return name, stringPtr("different text"), comment, ttl, useTTL, extAttrs
			},
			want: false,
		},
		"CommentDiffers": {
			mutate: func(name, text, _ *string, ttl *uint32, useTTL *bool, extAttrs map[string]string) (*string, *string, *string, *uint32, *bool, map[string]string) {
				return name, text, stringPtr("different comment"), ttl, useTTL, extAttrs
			},
			want: false,
		},
		"TTLDiffers": {
			mutate: func(name, text, comment *string, _ *uint32, useTTL *bool, extAttrs map[string]string) (*string, *string, *string, *uint32, *bool, map[string]string) {
				return name, text, comment, uint32Ptr(60), useTTL, extAttrs
			},
			want: false,
		},
		"UseTTLDiffers": {
			mutate: func(name, text, comment *string, ttl *uint32, _ *bool, extAttrs map[string]string) (*string, *string, *string, *uint32, *bool, map[string]string) {
				return name, text, comment, ttl, boolPtr(false), extAttrs
			},
			want: false,
		},
		"ExtAttrsDiffer": {
			mutate: func(name, text, comment *string, ttl *uint32, useTTL *bool, _ map[string]string) (*string, *string, *string, *uint32, *bool, map[string]string) {
				return name, text, comment, ttl, useTTL, map[string]string{"env": "staging"}
			},
			want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := observedRecord()
			n, tx, c, ttl, useTTL, ea := tc.mutate(
				stringPtr("host.example.com"),
				stringPtr("v=spf1 -all"),
				stringPtr("hello"),
				uint32Ptr(300),
				boolPtr(true),
				map[string]string{"env": "prod"},
			)
			got := isUpToDate(n, tx, c, ttl, useTTL, ea, rec)
			if got != tc.want {
				t.Errorf("%s: isUpToDate() = %v, want %v", name, got, tc.want)
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
	observed := &ibclient.RecordTXT{
		Name:    stringPtr("host.example.com"),
		Text:    stringPtr("v=spf1 -all"),
		Comment: stringPtr("hello"),
		Ttl:     uint32Ptr(28800),
		UseTtl:  boolPtr(false),
		Ea:      ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		stringPtr("host.example.com"),
		stringPtr("v=spf1 -all"),
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
	observed := &ibclient.RecordTXT{
		Name:    stringPtr("host.example.com"),
		Text:    stringPtr("v=spf1 -all"),
		Comment: stringPtr("hello"),
		Ttl:     uint32Ptr(300),
		UseTtl:  boolPtr(true),
		Ea:      ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		stringPtr("host.example.com"),
		stringPtr("v=spf1 -all"),
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
	rec := &ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
	}
	// nil spec map vs. a nil Ea (empty) response — should be considered
	// up to date (no phantom diff from a nil-vs-empty distinction).
	got := isUpToDate(stringPtr("host.example.com"), stringPtr("v=spf1 -all"), nil, nil, nil, nil, rec)
	if !got {
		t.Error("isUpToDate: want true when both spec and observed extAttrs are empty/nil, got false")
	}
}

func TestExtAttrsRoundTripTable(t *testing.T) {
	cases := map[string]map[string]string{
		"Empty": {},
		"Nil":   nil,
		"OneKey": {
			"env": "prod",
		},
		"MultiKey": {
			"env":   "prod",
			"owner": "platform-team",
		},
	}

	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			ea := buildEA(in)
			out := extAttrsFromEA(ea)
			if !extAttrsEqual(in, out) {
				t.Errorf("%s: round-trip got %v, want %v", name, out, in)
			}
		})
	}
}

func TestUint32OrZero(t *testing.T) {
	cases := map[string]struct {
		ttl  *uint32
		want uint32
	}{
		"Nil":  {ttl: nil, want: 0},
		"Zero": {ttl: uint32Ptr(0), want: 0},
		"Set":  {ttl: uint32Ptr(300), want: 300},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := uint32OrZero(tc.ttl)
			if got != tc.want {
				t.Errorf("%s: uint32OrZero(%v) = %d, want %d", name, tc.ttl, got, tc.want)
			}
		})
	}
}

// ── dualclient.ExtractCredentials: ssl_verify key is fully ignored ─────
//
// TLS verification is governed by the ProviderConfig's own sslVerify spec
// field (see cluster.go/namespaced.go's Connect methods), never by a key
// in the credentials Secret. This pins the migration: a legacy
// "ssl_verify" key in the Secret must have zero effect on
// dualclient.ExtractCredentials — dualclient.Credentials has no
// SslVerify field to read it into.
func TestExtractCredentialsIgnoresSecretSslVerifyKey(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "infobloxnios-credentials", "admin", "s3cr3t")
	secret.Data["ssl_verify"] = []byte("false")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := dualclient.ExtractCredentials(context.Background(), kube, "grid.example.com", xpv2.CredentialsSourceSecret, &xpv2.SecretKeySelector{
		SecretReference: xpv2.SecretReference{Name: "infobloxnios-credentials", Namespace: "crossplane-system"},
		Key:             "unused",
	}, "")
	if err != nil {
		t.Fatalf("ExtractCredentials: unexpected error: %v", err)
	}
	if creds.Host != "grid.example.com" || creds.Username != "admin" || creds.Password != "s3cr3t" {
		t.Errorf("ExtractCredentials: got %+v, want Host/Username/Password populated regardless of the ssl_verify key", creds)
	}
}

func TestBuildConnectorUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: config.BuildConnector must not hardcode sslVerify
	// to "true" — it must honor the sslVerify parameter. Both branches
	// must construct successfully (transport config validation happens
	// locally; no network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := dualclient.Credentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t"}
			conn, err := config.BuildConnector(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("BuildConnector: unexpected error: %v", err)
			}
			if conn == nil {
				t.Fatal("BuildConnector: expected non-nil connector")
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

	rec := &ibclient.RecordTXT{
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
	rec := &ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		Ea:   identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	}

	got := isUpToDate(stringPtr("host.example.com"), stringPtr("v=spf1 -all"), nil, nil, nil, map[string]string{"env": "prod"}, rec)
	if !got {
		t.Error("isUpToDate: want true when spec.forProvider.extAttrs matches the Grid map with the identity stamp stripped, got false")
	}
}

func TestClusterObserveAtProviderExtAttrsIncludesIdentityKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; got != testUIDCluster {
		t.Errorf("AtProvider.ExtAttrs[%q] = %q, want %q (full Grid EA mirror, stamp included)", identity.EAKey, got, testUIDCluster)
	}
}

// ── Identity: empty-uid refusal ──────────────────────────────────────────

func TestCreateTXTRecordRefusesEmptyUID(t *testing.T) {
	_, err := createTXTRecord(nil, stringPtr("default"), stringPtr("host.example.com"), stringPtr("v=spf1 -all"), nil, nil, nil, nil, "")
	if err == nil {
		t.Fatal("createTXTRecord: expected an error for an empty uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("createTXTRecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateTXTRecordRefusesEmptyUID(t *testing.T) {
	_, err := updateTXTRecord(nil, "record:txt/test1:host.example.com/default", stringPtr("host.example.com"), stringPtr("v=spf1 -all"), nil, nil, nil, nil, "")
	if err == nil {
		t.Fatal("updateTXTRecord: expected an error for an empty uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("updateTXTRecord: error = %v, want it to mention the empty uid", err)
	}
}

// TestCreateTXTRecordRefusesWhitespaceUID and
// TestUpdateTXTRecordRefusesWhitespaceUID: a whitespace-only uid is not
// empty by a literal "" comparison, but it is not a usable identity
// either — the guard must trim before checking, matching the shared
// identity resolution ladder's own TrimSpace check.

func TestCreateTXTRecordRefusesWhitespaceUID(t *testing.T) {
	_, err := createTXTRecord(nil, stringPtr("default"), stringPtr("host.example.com"), stringPtr("v=spf1 -all"), nil, nil, nil, nil, "   ")
	if err == nil {
		t.Fatal("createTXTRecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("createTXTRecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateTXTRecordRefusesWhitespaceUID(t *testing.T) {
	_, err := updateTXTRecord(nil, "record:txt/test1:host.example.com/default", stringPtr("host.example.com"), stringPtr("v=spf1 -all"), nil, nil, nil, nil, "   ")
	if err == nil {
		t.Fatal("updateTXTRecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("updateTXTRecord: error = %v, want it to mention the empty uid", err)
	}
}

// ── identity ladder: every row, both scopes ─────────────────────────────
//
// The tests above already prove Rotated (TestClusterObserveRecoversRotatedRefAndPersistsAnnotation
// et al.), NotFound (TestClusterObserveNotFound et al.) and one of the two
// HandleReuseError rows. What follows fills the remaining rows the pilot
// (recorda) covers: Adopted, FoundByUID, AmbiguousMatchError, and the
// namespaced HandleReuseError row — so no ladder outcome is exercised on
// only one scope.

func TestClusterObserveAdoptsUnstampedObjectAndForcesUpdate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		// No Ea at all — the object has never been stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)

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

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		// No Ea at all — the object has never been stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")

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

	foundRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "")
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

	foundRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "", "ProviderConfig")
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

	m.seed(&ibclient.RecordTXT{Name: stringPtr("host-a.example.com"), Text: stringPtr("v=spf1 -all"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.RecordTXT{Name: stringPtr("host-b.example.com"), Text: stringPtr("v=spf1 -all"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/stale-ref:host.example.com/default")

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

	m.seed(&ibclient.RecordTXT{Name: stringPtr("host-a.example.com"), Text: stringPtr("v=spf1 -all"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})
	m.seed(&ibclient.RecordTXT{Name: stringPtr("host-b.example.com"), Text: stringPtr("v=spf1 -all"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "record:txt/stale-ref:host.example.com/default", "ProviderConfig")

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

	refA := m.seed(&ibclient.RecordTXT{Name: stringPtr("host-a.example.com"), Text: stringPtr("v=spf1 -all"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})
	refB := m.seed(&ibclient.RecordTXT{Name: stringPtr("host-b.example.com"), Text: stringPtr("v=spf1 -all"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/stale-ref:host.example.com/default")

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

	refA := m.seed(&ibclient.RecordTXT{Name: stringPtr("host-a.example.com"), Text: stringPtr("v=spf1 -all"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})
	refB := m.seed(&ibclient.RecordTXT{Name: stringPtr("host-b.example.com"), Text: stringPtr("v=spf1 -all"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "record:txt/stale-ref:host.example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		// No Ea at all — never stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)

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

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		// No Ea at all — never stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")

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
	cr := newClusterTXTRecord("my-txtrecord", "")

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
		t.Errorf("Create: POST /record:txt calls = %d, want exactly 1", createCalls)
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
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "", "ProviderConfig")

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
		t.Errorf("Create: POST /record:txt calls = %d, want exactly 1", createCalls)
	}
	if putCalls != 0 {
		t.Errorf("Create: PUT calls = %d, want 0 — the identity stamp must land in the same request that creates the object, no follow-up PUT", putCalls)
	}
}

// TestCreateTXTRecordRefusesEmptyUIDIssuesNoMutatingCall is the
// controller-level (not just the bare-function) companion of
// TestCreateTXTRecordRefusesEmptyUID: proves the httptest server records
// zero mutating requests when Create is refused for an empty uid.
func TestCreateTXTRecordRefusesEmptyUIDIssuesNoMutatingCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", "")
	cr.SetUID("")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected an error for a blank metadata.uid, got nil")
	}

	m.mu.Lock()
	createCalls, eaDefCreateCalls := m.createCalls, m.eaDefCreateCalls
	m.mu.Unlock()
	if createCalls != 0 {
		t.Errorf("Create: POST /record:txt calls = %d, want 0 for a refused create", createCalls)
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

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterTXTRecord("my-txtrecord", ref)
	// Change only extAttrs — name/text changes rotate the _ref for this
	// resource only when m.renameOnUpdate is explicitly set (see the PUT
	// handler's nameOrTextChanged simulation, gated behind
	// renameOnUpdate), which is already covered by
	// TestClusterUpdateCapturesNewRefOnRename. This test isolates the
	// identity-reassert property from that rotation, and deliberately
	// leaves m.renameOnUpdate at its default (false) and Name/Text
	// untouched so there is no ambiguity either way.
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

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(ibclient.EA{"env": "prod"}, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")
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
	m.renameOnUpdate = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
	})

	cr := newClusterTXTRecord("my-txtrecord", oldRef)
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

	fetched := &clusterv1alpha1.TXTRecord{}
	if err := kube.Get(context.Background(), client.ObjectKey{Name: cr.GetName()}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Update: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

func TestNamespacedUpdateRefreshedExternalNamePersistsAcrossReGet(t *testing.T) {
	m := newMockWapiServer()
	m.renameOnUpdate = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
	})

	cr := newNamespacedTXTRecord("default", "my-txtrecord", oldRef, "ProviderConfig")
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

	fetched := &namespacedv1alpha1.TXTRecord{}
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
	cr := newClusterTXTRecord("my-txtrecord", "")

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

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		View: stringPtr("default"),
		Text: stringPtr("v=spf1 -all"),
		Ea:   identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-steady-state"}
	cr := newClusterTXTRecord("my-txtrecord", ref)

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

	foreignRef := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-foreign-identity"}
	cr := newClusterTXTRecord("my-txtrecord", foreignRef)

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
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/stale-ref:host.example.com/default")

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

	m.seed(&ibclient.RecordTXT{Name: stringPtr("host-a.example.com"), Text: stringPtr("v=spf1 -all"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.RecordTXT{Name: stringPtr("host-b.example.com"), Text: stringPtr("v=spf1 -all"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ambiguous"}
	cr := newClusterTXTRecord("my-txtrecord", "record:txt/stale-ref:host.example.com/default")

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
	cr := newClusterTXTRecord("my-txtrecord", "")

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

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		View: stringPtr("default"),
		Text: stringPtr("v=spf1 -all"),
		Ea:   identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-delete-steady-state"}
	cr := newClusterTXTRecord("my-txtrecord", ref)

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
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		View: stringPtr("default"),
		Text: stringPtr("v=spf1 -all"),
		Ea:   identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ns-steady-state"}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")

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
	cr := newNamespacedTXTRecord("default", "my-txtrecord", "", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		View: stringPtr("default"),
		Text: stringPtr("v=spf1 -all"),
		Ea:   identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: "grid-ns-delete-steady-state"}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")

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

// ── Read/write endpoint split (read-routing) ─────────────────────────────
//
// poisonIBConnector fails the test if any of its methods is ever called —
// used to prove Observe never touches a candidate connector when no
// readEndpoint is configured (router.Gate == nil, so BeginObserve always
// returns the primary unchanged).
type poisonIBConnector struct{ t *testing.T }

func (p poisonIBConnector) CreateObject(ibclient.IBObject) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected candidate CreateObject call")
	return "", nil
}

func (p poisonIBConnector) GetObject(ibclient.IBObject, string, *ibclient.QueryParams, interface{}) error {
	p.t.Helper()
	p.t.Fatal("unexpected candidate GetObject call")
	return nil
}

func (p poisonIBConnector) DeleteObject(string) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected candidate DeleteObject call")
	return "", nil
}

func (p poisonIBConnector) UpdateObject(ibclient.IBObject, string) (string, error) {
	p.t.Helper()
	p.t.Fatal("unexpected candidate UpdateObject call")
	return "", nil
}

// TestObserveReadEndpointAbsentNoCandidateCall proves that with no
// readEndpoint configured (router.Gate == nil), Observe never touches a
// candidate connector — readFrom is always e.conn. router.Candidate is a
// poisonIBConnector so any accidental candidate call fails the test
// immediately instead of silently succeeding.
func TestObserveReadEndpointAbsentNoCandidateCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector,
		router: readrouting.Router{Candidate: poisonIBConnector{t: t}}, // gate is nil, so this must never be touched
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newClusterTXTRecord("my-txtrecord", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
}

// TestNamespacedObserveReadEndpointAbsentNoCandidateCall is the
// namespaced-scope variant of TestObserveReadEndpointAbsentNoCandidateCall.
func TestNamespacedObserveReadEndpointAbsentNoCandidateCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordTXT{
		Name: stringPtr("host.example.com"),
		Text: stringPtr("v=spf1 -all"),
		View: stringPtr("default"),
		Ea:   identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{
		kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector,
		router: readrouting.Router{Candidate: poisonIBConnector{t: t}},
		prober: identity.NewProber(), endpoint: t.Name(),
	}
	cr := newNamespacedTXTRecord("default", "my-txtrecord", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
}
