// Package hostrecord unit tests for the HostRecord MR controllers. Tests
// use inline httptest.NewServer mocks that emulate the WAPI record:host
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package hostrecord

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/hostrecord/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/hostrecord/v1alpha1"
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

// newClusterHostRecord builds a minimal cluster-scoped HostRecord CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterHostRecord(crName, externalName string) *clusterv1alpha1.HostRecord {
	cr := &clusterv1alpha1.HostRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.HostRecordSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.HostRecordParameters{
				Name: stringPtr("host.example.com"),
				Ipv4Addrs: []clusterv1alpha1.HostRecordIpv4Addr{
					{Ipv4Addr: "10.0.0.1"},
				},
				NetworkView: stringPtr("default"),
				View:        stringPtr("default"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedHostRecord is the namespaced variant of newClusterHostRecord.
func newNamespacedHostRecord(ns, crName, externalName, pcKind string) *namespacedv1alpha1.HostRecord {
	cr := &namespacedv1alpha1.HostRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
		Spec: namespacedv1alpha1.HostRecordSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.HostRecordParameters{
				Name: stringPtr("host.example.com"),
				Ipv4Addrs: []namespacedv1alpha1.HostRecordIpv4Addr{
					{Ipv4Addr: "10.0.0.1"},
				},
				NetworkView: stringPtr("default"),
				View:        stringPtr("default"),
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
// mockWapiServer emulates the subset of NIOS WAPI record:host endpoints
// exercised by the HostRecord controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.HostRecord type so the wire format (including the EA
// {"value": ...} envelope) exactly matches what the SDK sends and expects.
// Unlike a real Grid Manager, GET always returns every field the mock has
// stored regardless of _return_fields — sufficient to exercise the
// controller's response parsing without reimplementing WAPI's field
// filtering.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.HostRecord
	nextRef int

	// searchCalls counts requests to the search endpoint (a GET with no
	// _ref path segment) — used to prove the identity ladder actually
	// issued a round trip rather than short-circuiting.
	searchCalls int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte

	// renameRefOnUpdate, when non-empty, simulates NIOS returning a
	// different _ref from the one addressed by PUT (e.g. a DNS-view or
	// name change on a live Grid Manager) — used to exercise the
	// controller's _ref instability handling on Update.
	renameRefOnUpdate string

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

	// createCalls/putCalls count POST/PUT requests against record:host
	// itself (independent of eaDefCreateCalls above), used to prove a
	// Create call issues exactly one mutating request — no follow-up PUT
	// to re-assert the identity stamp — and that a refused Create/Update
	// issues zero of either.
	createCalls int
	putCalls    int
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{
		records: map[string]*ibclient.HostRecord{},
		// The identity EA definition is present by default so every
		// pre-existing Create test sees the prerequisite as already
		// satisfied and never exercises the create-definition path.
		eaDefExists: true,
	}
}

func (m *mockWapiServer) seed(rec *ibclient.HostRecord) string {
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

func (m *mockWapiServer) newRefLocked(rec *ibclient.HostRecord) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	view := ""
	if rec.View != nil {
		view = *rec.View
	}
	return "record:host/test" + itoa(m.nextRef) + ":" + name + "/" + view
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

// handler returns an http.Handler implementing the record:host WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/record:host", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.HostRecord
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

	// Search endpoint (GetHostRecord, and the identity ladder's EA
	// search): a GET with no _ref path segment, filtered by
	// network_view/view/name/ipv4addr/ipv6addr query params, and/or a
	// "*<EA name>" extensible-attribute filter (the syntax
	// identity.Resolve's searchByUID uses). Registered as an exact
	// literal path so Go's ServeMux prefers it over the {ref...} wildcard
	// below for requests to precisely "record:host" (real _refs always
	// carry additional path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/record:host", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.searchCalls++
		m.mu.Unlock()

		q := r.URL.Query()
		networkView := q.Get("network_view")
		view := q.Get("view")
		name := q.Get("name")
		ipv4addr := q.Get("ipv4addr")
		ipv6addr := q.Get("ipv6addr")

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
		var matches []ibclient.HostRecord
		for _, rec := range m.records {
			if networkView != "" && rec.NetworkView != networkView {
				continue
			}
			if view != "" && (rec.View == nil || *rec.View != view) {
				continue
			}
			if name != "" && (rec.Name == nil || *rec.Name != name) {
				continue
			}
			if ipv4addr != "" {
				addr, _ := firstIpv4AddrAndMAC(ipv4AddrValuesFromSDK(rec.Ipv4Addrs))
				if addr != ipv4addr {
					continue
				}
			}
			if ipv6addr != "" {
				addr, _ := firstIpv6AddrAndDuid(ipv6AddrValuesFromSDK(rec.Ipv6Addrs))
				if addr != ipv6addr {
					continue
				}
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
		var incoming ibclient.HostRecord
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.putCalls++
		m.lastUpdateBody = body
		existing.Name = incoming.Name
		existing.Ipv4Addrs = incoming.Ipv4Addrs
		existing.Ipv6Addrs = incoming.Ipv6Addrs
		existing.View = incoming.View
		existing.Aliases = incoming.Aliases
		existing.EnableDns = incoming.EnableDns
		existing.Comment = incoming.Comment
		existing.Disable = incoming.Disable
		existing.Ttl = incoming.Ttl
		existing.UseTtl = incoming.UseTtl
		existing.Ea = incoming.Ea
		existing.Zone = zoneFromName(existing.Name)

		// Simulate a live Grid Manager assigning a new _ref on update
		// (e.g. DNS-view or name change) rather than echoing the ref
		// the request addressed.
		newRef := ref
		if m.renameRefOnUpdate != "" {
			newRef = m.renameRefOnUpdate
		}
		if newRef != ref {
			existing.Ref = newRef
			delete(m.records, ref)
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

// newTestClient builds a hostRecordClient pointed at the given
// httptest.Server via plain HTTP (no TLS needed — the WapiRequestBuilder
// only switches to HTTPS when hostCfg.Scheme != "http").
func newTestClient(t *testing.T, srv *httptest.Server) *hostRecordClient {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	hc, err := newHostRecordClientWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test host record client: %v", err)
	}
	// prober is set to a fresh instance (not nil) so tests never share
	// identity.DefaultProber's process-wide TTL cache across httptest
	// servers — otherwise one test's cached verdict would leak into
	// another's assertions about the prerequisite probe. endpoint is
	// likewise set to a value unique to this test/subtest (rather than
	// left empty, which would fall back to the shared
	// unresolvedProbeEndpoint cache key) so no two tests can ever
	// collide on the same cache row even if a future change reused a
	// Prober across calls within one test.
	hc.prober = identity.NewProber()
	hc.endpoint = t.Name()
	return hc
}

// ── cluster: Observe ────────────────────────────────────────────────────

func TestClusterObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Comment:     stringPtr("hello"),
		Ttl:         uint32Ptr(300),
		UseTtl:      boolPtr(true),
		Ea:          identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
		Disable:     boolPtr(false),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
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
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/does-not-exist:host.example.com/default")

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

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())          // simulate NameAsExternalName initializer

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

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, an empty string
// NetworkView, a nil Ea map, nil slices) must not panic and must produce a
// valid observation with nil-safe AtProvider fields.
// observeFromHostRecord copies optional pointer fields directly (never
// dereferences without a nil guard first), so this test also pins that
// contract for future edits.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// Seed a completely bare HostRecord — only the SDK-assigned _ref (via
	// seed()) identifies the object. Name/View are nil, so zoneFromName
	// leaves Zone at "" too.
	ref := m.seed(&ibclient.HostRecord{})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)

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
	if ap.Ipv4Addrs != nil {
		t.Errorf("AtProvider.Ipv4Addrs = %v, want nil", ap.Ipv4Addrs)
	}
	if ap.Ipv6Addrs != nil {
		t.Errorf("AtProvider.Ipv6Addrs = %v, want nil", ap.Ipv6Addrs)
	}
	if ap.NetworkView != nil {
		t.Errorf("AtProvider.NetworkView = %v, want nil", ap.NetworkView)
	}
	if ap.View != nil {
		t.Errorf("AtProvider.View = %v, want nil", ap.View)
	}
	if ap.Aliases != nil {
		t.Errorf("AtProvider.Aliases = %v, want nil", ap.Aliases)
	}
	if ap.ConfigureForDNS != nil {
		t.Errorf("AtProvider.ConfigureForDNS = %v, want nil", ap.ConfigureForDNS)
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
	if ap.Ref == nil || *ap.Ref != ref {
		t.Errorf("AtProvider.Ref = %v, want %q", ap.Ref, ref)
	}
	if ap.Zone != nil {
		t.Errorf("AtProvider.Zone = %v, want nil", ap.Zone)
	}
	if ap.DNSName != nil {
		t.Errorf("AtProvider.DNSName = %v, want nil", ap.DNSName)
	}
	if ap.DNSAliases != nil {
		t.Errorf("AtProvider.DNSAliases = %v, want nil", ap.DNSAliases)
	}
}

// TestClusterObserveFullMirror verifies that response-only fields not
// present in HostRecordParameters — Disable, DNSName, DNSAliases — are
// correctly mirrored into AtProvider (full-mirror AtProvider convention).
// These three fields fall outside the ObjectManager.GetHostRecordByRef
// wrapper's fixed default return-field set, so this test also pins the
// newEmptyHostRecordForIdentity field-set extension (see its doc comment).
func TestClusterObserveFullMirror(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Disable:     boolPtr(true),
		DnsName:     "xn--host.example.com",
		DnsAliases:  []string{"alias.example.com"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	ap := cr.Status.AtProvider
	if ap.Disable == nil || !*ap.Disable {
		t.Errorf("AtProvider.Disable = %v, want true", ap.Disable)
	}
	if ap.DNSName == nil || *ap.DNSName != "xn--host.example.com" {
		t.Errorf("AtProvider.DNSName = %v, want xn--host.example.com", ap.DNSName)
	}
	if len(ap.DNSAliases) != 1 || ap.DNSAliases[0] != "alias.example.com" {
		t.Errorf("AtProvider.DNSAliases = %v, want [alias.example.com]", ap.DNSAliases)
	}
	if ap.NetworkView == nil || *ap.NetworkView != "default" {
		t.Errorf("AtProvider.NetworkView = %v, want default", ap.NetworkView)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "") // no external-name yet

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
// create endpoint is propagated (wrapped, not swallowed).
func TestClusterCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateHostRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateHostRecord)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "original-network-view",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
	// Mutate the immutable networkView field in spec — this must NOT
	// affect ResourceUpToDate, since networkView is excluded from
	// isUpToDate (WAPI rejects updates to it).
	cr.Spec.ForProvider.NetworkView = stringPtr("changed-network-view")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite networkView drift (immutable field), got false")
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Comment:     stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
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

// TestClusterUpdatePrerequisiteAutoCreates verifies ADR-IN-0006 §6's
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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Comment:     stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
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

// TestClusterUpdatePrerequisiteRefusesUncreatable verifies ADR-IN-0006
// §6's unconditional Update guard on the refusal side: when the identity
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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Comment:     stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)

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
	if v, present := raw["network_view"]; present && v != "" {
		t.Errorf("Update: request body contains immutable field 'network_view': %v", v)
	}
}

// TestClusterUpdateServerError verifies that a 5xx response from the WAPI
// is propagated (wrapped, not swallowed). A totally unresponsive Grid
// fails at the identity-prerequisite probe stage (issued unconditionally
// before the update call itself, mirroring Create).
func TestClusterUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/test1:host.example.com/default")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateHostRecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateHostRecord)
	}
}

// TestClusterUpdateRefChange pins the _ref instability handling documented
// on clusterExternal.Update: live verification against a real NIOS Grid
// Manager showed that renaming a host record (or changing its DNS view)
// changes its _ref, so the controller must refresh the external-name
// annotation from the server-returned ref rather than assuming it is
// stable across Update calls.
func TestClusterUpdateRefChange(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	})
	newRef := "record:host/test999:host.example.com/other"
	m.renameRefOnUpdate = newRef

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", oldRef)
	cr.Spec.ForProvider.View = stringPtr("other")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	if got := meta.GetExternalName(cr); got != newRef {
		t.Errorf("Update: external-name annotation = %q, want %q (refreshed from server-returned _ref)", got, newRef)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{Name: stringPtr("host.example.com"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)

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

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/does-not-exist:host.example.com/default")

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

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/test1:host.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteHostRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteHostRecord)
	}
}

// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject verifies the
// core defect fix: a 404 against the stored _ref must not be treated as
// "already deleted" when a natural-key search finds the same identity
// still live under a different _ref. Deleting that record would be
// unverifiable ownership, so Delete() must refuse and leave the record
// in place.
func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/stale-ref:host.example.com/default")

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

	foreignRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, "someone-elses-uid"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", foreignRef)

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

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/stale-ref:host.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
	}
}

// TestClusterObserveRecoversRotatedRefAndPersistsAnnotation is the
// Observe()-side counterpart: crossplane-runtime's managed reconciler
// calls Observe() before Delete() on the deletion path, and if Observe()
// reports ResourceExists:false the reconciler never calls Delete() at
// all — it just clears the finalizer, orphaning the Grid object. The
// identity ladder recovers the rotated reference here too, and Observe
// must persist it via ResourceLateInitialized so a later reconcile does
// not repeat the search.
func TestClusterObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/stale-ref:host.example.com/default")

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

// TestClusterObserveRefusesOnForeignIdentity verifies that Observe
// surfaces a HandleReuseError (Synced=False, no mutating call) when the
// stored _ref resolves to an object whose identity attribute belongs to
// a different owner.
func TestClusterObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, "someone-elses-uid"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", foreignRef)

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

	cr := newClusterHostRecord("my-hostrecord", "")
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

	cr := newClusterHostRecord("my-hostrecord", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/does-not-exist:host.example.com/default", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "", "ProviderConfig")
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/test1:host.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/test1:host.example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.HostRecord{})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")

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
	if ap.Ipv4Addrs != nil {
		t.Errorf("AtProvider.Ipv4Addrs = %v, want nil", ap.Ipv4Addrs)
	}
	if ap.Ipv6Addrs != nil {
		t.Errorf("AtProvider.Ipv6Addrs = %v, want nil", ap.Ipv6Addrs)
	}
	if ap.NetworkView != nil {
		t.Errorf("AtProvider.NetworkView = %v, want nil", ap.NetworkView)
	}
	if ap.View != nil {
		t.Errorf("AtProvider.View = %v, want nil", ap.View)
	}
	if ap.Aliases != nil {
		t.Errorf("AtProvider.Aliases = %v, want nil", ap.Aliases)
	}
	if ap.ConfigureForDNS != nil {
		t.Errorf("AtProvider.ConfigureForDNS = %v, want nil", ap.ConfigureForDNS)
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
	if ap.Ref == nil || *ap.Ref != ref {
		t.Errorf("AtProvider.Ref = %v, want %q", ap.Ref, ref)
	}
	if ap.Zone != nil {
		t.Errorf("AtProvider.Zone = %v, want nil", ap.Zone)
	}
	if ap.DNSName != nil {
		t.Errorf("AtProvider.DNSName = %v, want nil", ap.DNSName)
	}
	if ap.DNSAliases != nil {
		t.Errorf("AtProvider.DNSAliases = %v, want nil", ap.DNSAliases)
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateServerError verifies that a 5xx response from the
// WAPI create endpoint is propagated (wrapped, not swallowed).
func TestNamespacedCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateHostRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateHostRecord)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.Ipv4Addrs = []namespacedv1alpha1.HostRecordIpv4Addr{{Ipv4Addr: "10.0.0.2"}}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if len(stored.Ipv4Addrs) != 1 || stored.Ipv4Addrs[0].Ipv4Addr == nil || *stored.Ipv4Addrs[0].Ipv4Addr != "10.0.0.2" {
		t.Errorf("Update: stored ipv4addrs = %v, want [10.0.0.2]", stored.Ipv4Addrs)
	}
}

// TestNamespacedUpdatePrerequisiteAutoCreates verifies ADR-IN-0006 §6's
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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.Ipv4Addrs = []namespacedv1alpha1.HostRecordIpv4Addr{{Ipv4Addr: "10.0.0.2"}}

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

// TestNamespacedUpdatePrerequisiteRefusesUncreatable verifies ADR-IN-0006
// §6's unconditional Update guard on the refusal side: when the identity
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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.Ipv4Addrs = []namespacedv1alpha1.HostRecordIpv4Addr{{Ipv4Addr: "10.0.0.2"}}

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
// WAPI is propagated (wrapped, not swallowed). A totally unresponsive
// Grid fails at the identity-prerequisite probe stage (issued
// unconditionally before the update call itself, mirroring Create).
func TestNamespacedUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/test1:host.example.com/default", "ProviderConfig")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateHostRecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateHostRecord)
	}
}

// TestNamespacedUpdateRefChange mirrors TestClusterUpdateRefChange for the
// namespaced scope — the server-returned _ref must be re-adopted as the
// external-name annotation when it differs from the ref addressed.
func TestNamespacedUpdateRefChange(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	})
	newRef := "record:host/test999:host.example.com/other"
	m.renameRefOnUpdate = newRef

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", oldRef, "ProviderConfig")
	cr.Spec.ForProvider.View = stringPtr("other")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	if got := meta.GetExternalName(cr); got != newRef {
		t.Errorf("Update: external-name annotation = %q, want %q (refreshed from server-returned _ref)", got, newRef)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{Name: stringPtr("host.example.com"), View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/does-not-exist:host.example.com/default", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/test1:host.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteHostRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteHostRecord)
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, testUIDNamespaced),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/stale-ref:host.example.com/default", "ProviderConfig")

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

// TestNamespacedObserveRecoversRotatedRefAndPersistsAnnotation is the
// namespaced-scope counterpart of
// TestClusterObserveRecoversRotatedRefAndPersistsAnnotation.
func TestNamespacedObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, testUIDNamespaced),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/stale-ref:host.example.com/default", "ProviderConfig")

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

// TestNamespacedObserveRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterObserveRefusesOnForeignIdentity.
func TestNamespacedObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, "someone-elses-uid"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", foreignRef, "ProviderConfig")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Observe: error = %v, want it to wrap a *identity.HandleReuseError", err)
	}
}

// TestNamespacedDeleteRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterDeleteRefusesOnForeignIdentity.
func TestNamespacedDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, "someone-elses-uid"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", foreignRef, "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/stale-ref:host.example.com/default", "ProviderConfig")

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

	cr := newNamespacedHostRecord(ns, "my-hostrecord", "", "ProviderConfig")
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

	cr := newNamespacedHostRecord("app-ns", "my-hostrecord", "", "ClusterProviderConfig")
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

	cr := newNamespacedHostRecord("default", "my-hostrecord", "", "SomeOtherKind")
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
	var view *string
	var configureForDNS *bool
	var disable *bool
	extAttrs := map[string]string(nil)
	aliases := []string(nil)
	ipv4Addrs := []ipv4AddrValue(nil)
	ipv6Addrs := []ipv6AddrValue(nil)

	rec := &ibclient.HostRecord{
		Comment:   stringPtr("server default"),
		Ttl:       uint32Ptr(600),
		UseTtl:    boolPtr(true),
		Ea:        ibclient.EA{"env": "prod"},
		View:      stringPtr("observed-view"),
		EnableDns: boolPtr(true),
		Disable:   boolPtr(true),
		Aliases:   []string{"alias.example.com"},
		Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.50")}},
		Ipv6Addrs: []ibclient.HostRecordIpv6Addr{{Ipv6Addr: stringPtr("2001:db8::1")}},
	}

	changed := lateInitialize(&comment, &ttl, &useTTL, &extAttrs, &view, &configureForDNS, &disable, &aliases, &ipv4Addrs, &ipv6Addrs, rec)
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
	if view == nil || *view != "observed-view" {
		t.Errorf("lateInitialize: view = %v, want observed-view", view)
	}
	if configureForDNS == nil || !*configureForDNS {
		t.Errorf("lateInitialize: configureForDNS = %v, want true", configureForDNS)
	}
	if disable == nil || !*disable {
		t.Errorf("lateInitialize: disable = %v, want true", disable)
	}
	if len(ipv4Addrs) != 1 || ipv4Addrs[0].Ipv4Addr != "10.0.0.50" {
		t.Errorf("lateInitialize: ipv4Addrs = %v, want [10.0.0.50]", ipv4Addrs)
	}
	if len(ipv6Addrs) != 1 || ipv6Addrs[0].Ipv6Addr != "2001:db8::1" {
		t.Errorf("lateInitialize: ipv6Addrs = %v, want [2001:db8::1]", ipv6Addrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	ttl := uint32Ptr(120)
	useTTL := boolPtr(false)
	view := stringPtr("user-view")
	configureForDNS := boolPtr(false)
	disable := boolPtr(false)
	extAttrs := map[string]string{"env": "staging"}
	aliases := []string{"user-alias.example.com"}
	ipv4Addrs := []ipv4AddrValue{{Ipv4Addr: "10.0.0.9"}}
	ipv6Addrs := []ipv6AddrValue{{Ipv6Addr: "2001:db8::9"}}

	rec := &ibclient.HostRecord{
		Comment:   stringPtr("server default"),
		Ttl:       uint32Ptr(600),
		UseTtl:    boolPtr(true),
		Ea:        ibclient.EA{"env": "prod"},
		View:      stringPtr("observed-view"),
		EnableDns: boolPtr(true),
		Disable:   boolPtr(true),
		Aliases:   []string{"alias.example.com"},
		Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.50")}},
		Ipv6Addrs: []ibclient.HostRecordIpv6Addr{{Ipv6Addr: stringPtr("2001:db8::1")}},
	}

	changed := lateInitialize(&comment, &ttl, &useTTL, &extAttrs, &view, &configureForDNS, &disable, &aliases, &ipv4Addrs, &ipv6Addrs, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" || *ttl != 120 || *useTTL != false || extAttrs["env"] != "staging" {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
	if *view != "user-view" || *configureForDNS != false || *disable != false {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
	if len(ipv4Addrs) != 1 || ipv4Addrs[0].Ipv4Addr != "10.0.0.9" {
		t.Error("lateInitialize: overwrote already-set ipv4Addrs")
	}
	if len(ipv6Addrs) != 1 || ipv6Addrs[0].Ipv6Addr != "2001:db8::9" {
		t.Error("lateInitialize: overwrote already-set ipv6Addrs")
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that Name — the
// only field with no valid "empty" state — is never overwritten by
// Observe()'s late-init step, and that a non-empty Ipv4Addrs (the static-
// address case) is left alone too. Ipv4Addrs/Ipv6Addrs are eligible for
// late-init only when still empty in spec (the dynamic-allocation case —
// see TestClusterCreateWithIpv4Cidr and TestClusterCreateWithFilterParams
// for that path); this test drives the already-set case to pin the
// non-overwrite guarantee.
func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("observed.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.99")}},
		NetworkView: "observed-network-view",
		View:        stringPtr("observed-view"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
	cr.Spec.ForProvider.Name = stringPtr("host.example.com")
	cr.Spec.ForProvider.Ipv4Addrs = []clusterv1alpha1.HostRecordIpv4Addr{{Ipv4Addr: "10.0.0.1"}}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := *cr.Spec.ForProvider.Name; got != "host.example.com" {
		t.Errorf("Observe: required field Name late-initialized to %q, want unchanged %q", got, "host.example.com")
	}
	if got := cr.Spec.ForProvider.Ipv4Addrs[0].Ipv4Addr; got != "10.0.0.1" {
		t.Errorf("Observe: required field Ipv4Addrs late-initialized to %q, want unchanged %q", got, "10.0.0.1")
	}
}

// ── multi-address SDK limitation (documented, tested) ────────────────────
//
// TestCreateOnlyForwardsFirstIpv4Addr pins the documented SDK limitation
// described on ipv4AddrsEqual: CreateHostRecord accepts only a single
// scalar ipv4Addr, so a second Ipv4Addrs entry in spec is silently not
// forwarded to WAPI.
func TestCreateOnlyForwardsFirstIpv4Addr(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
	cr.Spec.ForProvider.Ipv4Addrs = []clusterv1alpha1.HostRecordIpv4Addr{
		{Ipv4Addr: "10.0.0.1"},
		{Ipv4Addr: "10.0.0.2"},
	}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if len(stored.Ipv4Addrs) != 1 {
		t.Fatalf("Create: stored ipv4Addrs count = %d, want 1 (documented SDK limitation)", len(stored.Ipv4Addrs))
	}
	if stored.Ipv4Addrs[0].Ipv4Addr == nil || *stored.Ipv4Addrs[0].Ipv4Addr != "10.0.0.1" {
		t.Errorf("Create: stored first ipv4addr = %v, want 10.0.0.1", stored.Ipv4Addrs[0].Ipv4Addr)
	}
}

// ── isUpToDate / lateInitialize: useTtl gating ──────────────────────────

// TestIsUpToDateIgnoresTTLWhenUseTTLOff proves the ttl comparison is
// gated on useTtl. When useTtl is false, WAPI ignores the submitted ttl
// and returns the zone default (a realistic non-zero value, not 0) on
// every GET — the spec ttl and the observed ttl are unrelated
// quantities, and comparing them unconditionally can never converge.
func TestIsUpToDateIgnoresTTLWhenUseTTLOff(t *testing.T) {
	zoneDefault := uint32(28800)
	observed := &ibclient.HostRecord{
		Name:      stringPtr("host.example.com"),
		View:      stringPtr("default"),
		Ttl:       &zoneDefault,
		UseTtl:    boolPtr(false),
		EnableDns: boolPtr(true),
	}

	p := hostRecordCompareFields{
		Name:            stringPtr("host.example.com"),
		View:            stringPtr("default"),
		ConfigureForDNS: boolPtr(true),
		TTL:             uint32Ptr(0),
		UseTTL:          boolPtr(false),
	}

	if !isUpToDate(p, observed) {
		t.Error("isUpToDate: want true when useTtl is off and only the server-owned ttl differs, got false (non-convergent drift comparison)")
	}
}

// TestIsUpToDateDetectsUseTTLTransition proves a useTtl true -> false
// transition is still detected as drift even though the value comparison
// is gated off. The flag comparison must be unconditional.
func TestIsUpToDateDetectsUseTTLTransition(t *testing.T) {
	ttl := uint32(300)
	observed := &ibclient.HostRecord{
		Name:      stringPtr("host.example.com"),
		View:      stringPtr("default"),
		Ttl:       &ttl,
		UseTtl:    boolPtr(true),
		EnableDns: boolPtr(true),
	}

	p := hostRecordCompareFields{
		Name:            stringPtr("host.example.com"),
		View:            stringPtr("default"),
		ConfigureForDNS: boolPtr(true),
		TTL:             uint32Ptr(300),
		UseTTL:          boolPtr(false),
	}

	if isUpToDate(p, observed) {
		t.Error("isUpToDate: want false on a useTtl true -> false transition, got true (drift not detected)")
	}
}

// TestLateInitializeDoesNotBackfillTTLWhenUseTTLOff proves that when
// useTtl is false the observed ttl (WAPI's zone default, not a value the
// user's config implies) is never written back into spec.forProvider.ttl.
func TestLateInitializeDoesNotBackfillTTLWhenUseTTLOff(t *testing.T) {
	var comment, view *string
	var ttl *uint32
	useTTL := boolPtr(false)
	extAttrs := map[string]string(nil)
	var configureForDNS, disable *bool
	var aliases []string
	var ipv4Addrs []ipv4AddrValue
	var ipv6Addrs []ipv6AddrValue

	zoneDefault := uint32(28800)
	rec := &ibclient.HostRecord{
		Ttl:    &zoneDefault,
		UseTtl: boolPtr(false),
	}

	lateInitialize(&comment, &ttl, &useTTL, &extAttrs, &view, &configureForDNS, &disable, &aliases, &ipv4Addrs, &ipv6Addrs, rec)

	if ttl != nil {
		t.Errorf("lateInitialize: ttl = %v, want nil (useTtl is off, observed ttl is the zone default, not a user value)", *ttl)
	}
}

// ── dynamic IP allocation: validation ────────────────────────────────────

func TestValidateHostRecordAllocationRejectsIpv4CidrWithStaticAddr(t *testing.T) {
	p := hostRecordCompareFields{Ipv4Addrs: []ipv4AddrValue{{Ipv4Addr: "10.0.0.1"}}}
	err := validateHostRecordAllocation(p, stringPtr("10.0.0.0/24"), nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), errIpv4CidrWithStaticAddr) {
		t.Errorf("validateHostRecordAllocation: err = %v, want it to contain %q", err, errIpv4CidrWithStaticAddr)
	}
}

func TestValidateHostRecordAllocationRejectsIpv6CidrWithStaticAddr(t *testing.T) {
	p := hostRecordCompareFields{Ipv6Addrs: []ipv6AddrValue{{Ipv6Addr: "2001:db8::1"}}}
	err := validateHostRecordAllocation(p, nil, stringPtr("2001:db8::/64"), nil, nil)
	if err == nil || !strings.Contains(err.Error(), errIpv6CidrWithStaticAddr) {
		t.Errorf("validateHostRecordAllocation: err = %v, want it to contain %q", err, errIpv6CidrWithStaticAddr)
	}
}

func TestValidateHostRecordAllocationRejectsFilterParamsWithCidr(t *testing.T) {
	p := hostRecordCompareFields{}
	err := validateHostRecordAllocation(p, stringPtr("10.0.0.0/24"), nil, map[string]string{"*Site": "HQ"}, stringPtr("IPV4"))
	if err == nil || !strings.Contains(err.Error(), errFilterParamsWithCidr) {
		t.Errorf("validateHostRecordAllocation: err = %v, want it to contain %q", err, errFilterParamsWithCidr)
	}
}

func TestValidateHostRecordAllocationRequiresIpAddressType(t *testing.T) {
	p := hostRecordCompareFields{}
	err := validateHostRecordAllocation(p, nil, nil, map[string]string{"*Site": "HQ"}, nil)
	if err == nil || !strings.Contains(err.Error(), errFilterParamsRequiresType) {
		t.Errorf("validateHostRecordAllocation: err = %v, want it to contain %q", err, errFilterParamsRequiresType)
	}
}

func TestValidateHostRecordAllocationAllowsCidrAlone(t *testing.T) {
	p := hostRecordCompareFields{}
	if err := validateHostRecordAllocation(p, stringPtr("10.0.0.0/24"), stringPtr("2001:db8::/64"), nil, nil); err != nil {
		t.Errorf("validateHostRecordAllocation: unexpected error for dual-stack CIDR allocation: %v", err)
	}
}

func TestValidateHostRecordAllocationAllowsFilterParamsAlone(t *testing.T) {
	p := hostRecordCompareFields{}
	if err := validateHostRecordAllocation(p, nil, nil, map[string]string{"*Site": "HQ"}, stringPtr("IPV4")); err != nil {
		t.Errorf("validateHostRecordAllocation: unexpected error for EA-filter allocation: %v", err)
	}
}

// ── dynamic IP allocation: CIDR-based (Path 1 — CreateHostRecord) ───────

// TestClusterCreateWithIpv4Cidr verifies that a HostRecord with ipv4Cidr
// set (and no static ipv4Addrs) forwards the CIDR to CreateHostRecord,
// which substitutes a func:nextavailableip expression for the address —
// the request the mock WAPI server actually receives.
func TestClusterCreateWithIpv4Cidr(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
	cr.Spec.ForProvider.Ipv4Addrs = nil
	cr.Spec.ForProvider.Ipv4Cidr = stringPtr("10.0.0.0/24")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if len(stored.Ipv4Addrs) != 1 || stored.Ipv4Addrs[0].Ipv4Addr == nil {
		t.Fatalf("Create: stored ipv4Addrs = %v, want one entry", stored.Ipv4Addrs)
	}
	want := "func:nextavailableip:10.0.0.0/24,default"
	if got := *stored.Ipv4Addrs[0].Ipv4Addr; got != want {
		t.Errorf("Create: stored ipv4addr = %q, want %q", got, want)
	}
}

// TestClusterCreateRejectsIpv4CidrWithStaticAddr verifies Create surfaces
// the validation error (wrapped) when ipv4Cidr and a static ipv4Addrs
// entry are both set.
func TestClusterCreateRejectsIpv4CidrWithStaticAddr(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
	cr.Spec.ForProvider.Ipv4Cidr = stringPtr("10.0.0.0/24")
	// newClusterHostRecord already sets a static 10.0.0.1 ipv4Addrs entry.

	_, err := e.Create(context.Background(), cr)
	if err == nil || !strings.Contains(err.Error(), errIpv4CidrWithStaticAddr) {
		t.Errorf("Create: err = %v, want it to contain %q", err, errIpv4CidrWithStaticAddr)
	}
}

// TestClusterCreateStaticAddrsStillWorks is a regression guard: a
// HostRecord with only static ipv4Addrs (no ipv4Cidr/ipv6Cidr set) must
// still forward the literal address to CreateHostRecord unchanged — not
// a func:nextavailableip expression — so wiring the CIDR-based
// allocation path did not disturb the pre-existing static-address path.
func TestClusterCreateStaticAddrsStillWorks(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "") // no external-name yet
	// newClusterHostRecord already sets a static 10.0.0.1 ipv4Addrs entry
	// and leaves ipv4Cidr/ipv6Cidr unset.

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if len(stored.Ipv4Addrs) != 1 || stored.Ipv4Addrs[0].Ipv4Addr == nil {
		t.Fatalf("Create: stored ipv4Addrs = %v, want one entry", stored.Ipv4Addrs)
	}
	if got := *stored.Ipv4Addrs[0].Ipv4Addr; got != "10.0.0.1" {
		t.Errorf("Create: stored ipv4addr = %q, want static address 10.0.0.1 (unmodified)", got)
	}
}

// TestClusterCreateWithIpv6CidrAllocatesIP is the IPv6 counterpart of
// TestClusterCreateWithIpv4Cidr: a HostRecord with ipv6Cidr set (and no
// static ipv6Addrs) forwards the CIDR to CreateHostRecord, which
// substitutes a func:nextavailableip expression for the address.
func TestClusterCreateWithIpv6CidrAllocatesIP(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
	cr.Spec.ForProvider.Ipv4Addrs = nil
	cr.Spec.ForProvider.Ipv6Cidr = stringPtr("2001:db8::/64")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if len(stored.Ipv6Addrs) != 1 || stored.Ipv6Addrs[0].Ipv6Addr == nil {
		t.Fatalf("Create: stored ipv6Addrs = %v, want one entry", stored.Ipv6Addrs)
	}
	want := "func:nextavailableip:2001:db8::/64,default"
	if got := *stored.Ipv6Addrs[0].Ipv6Addr; got != want {
		t.Errorf("Create: stored ipv6addr = %q, want %q", got, want)
	}
}

// TestClusterCreateWithDualStackCidrs verifies that a HostRecord with
// both ipv4Cidr and ipv6Cidr set forwards both CIDRs to CreateHostRecord
// in a single call, allocating an address from each family.
func TestClusterCreateWithDualStackCidrs(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
	cr.Spec.ForProvider.Ipv4Addrs = nil
	cr.Spec.ForProvider.Ipv4Cidr = stringPtr("10.0.0.0/24")
	cr.Spec.ForProvider.Ipv6Cidr = stringPtr("2001:db8::/64")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if len(stored.Ipv4Addrs) != 1 || stored.Ipv4Addrs[0].Ipv4Addr == nil {
		t.Fatalf("Create: stored ipv4Addrs = %v, want one entry", stored.Ipv4Addrs)
	}
	if want, got := "func:nextavailableip:10.0.0.0/24,default", *stored.Ipv4Addrs[0].Ipv4Addr; got != want {
		t.Errorf("Create: stored ipv4addr = %q, want %q", got, want)
	}
	if len(stored.Ipv6Addrs) != 1 || stored.Ipv6Addrs[0].Ipv6Addr == nil {
		t.Fatalf("Create: stored ipv6Addrs = %v, want one entry", stored.Ipv6Addrs)
	}
	if want, got := "func:nextavailableip:2001:db8::/64,default", *stored.Ipv6Addrs[0].Ipv6Addr; got != want {
		t.Errorf("Create: stored ipv6addr = %q, want %q", got, want)
	}
}

// TestClusterCreateRejectsIpv6CidrWithStaticAddr is the IPv6 counterpart
// of TestClusterCreateRejectsIpv4CidrWithStaticAddr: Create surfaces the
// validation error (wrapped) when ipv6Cidr and a static ipv6Addrs entry
// are both set.
func TestClusterCreateRejectsIpv6CidrWithStaticAddr(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
	cr.Spec.ForProvider.Ipv6Cidr = stringPtr("2001:db8::/64")
	cr.Spec.ForProvider.Ipv6Addrs = []clusterv1alpha1.HostRecordIpv6Addr{
		{Ipv6Addr: "2001:db8::1"},
	}

	_, err := e.Create(context.Background(), cr)
	if err == nil || !strings.Contains(err.Error(), errIpv6CidrWithStaticAddr) {
		t.Errorf("Create: err = %v, want it to contain %q", err, errIpv6CidrWithStaticAddr)
	}
}

// TestClusterCreateRejectsFilterParamsAndCidr verifies Create surfaces
// the validation error (wrapped) when filterParams and ipv4Cidr are both
// set — the EA-filter-based and CIDR-based allocation strategies are
// mutually exclusive.
func TestClusterCreateRejectsFilterParamsAndCidr(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
	cr.Spec.ForProvider.Ipv4Addrs = nil
	cr.Spec.ForProvider.Ipv4Cidr = stringPtr("10.0.0.0/24")
	cr.Spec.ForProvider.FilterParams = map[string]string{"*Site": "HQ"}
	cr.Spec.ForProvider.IpAddressType = stringPtr("IPV4")

	_, err := e.Create(context.Background(), cr)
	if err == nil || !strings.Contains(err.Error(), errFilterParamsWithCidr) {
		t.Errorf("Create: err = %v, want it to contain %q", err, errFilterParamsWithCidr)
	}
}

// TestClusterObserveLateInitializesIpv4AddrsAfterCidrAllocation verifies
// that once a CIDR-allocated HostRecord is observed, the WAPI-assigned
// address is captured back into spec.ipv4Addrs (empty at apply-time,
// since the user relied on ipv4Cidr rather than a static address) so
// later reconciles compare against a stable value instead of leaving
// ResourceUpToDate permanently false.
func TestClusterObserveLateInitializesIpv4AddrsAfterCidrAllocation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.55")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
	cr.Spec.ForProvider.Ipv4Addrs = nil
	cr.Spec.ForProvider.Ipv4Cidr = stringPtr("10.0.0.0/24")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true, got false")
	}
	if len(cr.Spec.ForProvider.Ipv4Addrs) != 1 || cr.Spec.ForProvider.Ipv4Addrs[0].Ipv4Addr != "10.0.0.55" {
		t.Errorf("Observe: spec.ipv4Addrs = %v, want [10.0.0.55]", cr.Spec.ForProvider.Ipv4Addrs)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true once ipv4Addrs is late-initialized, got false")
	}
}

// TestClusterObserveLateInitializesIpv6AddrsAfterCidrAllocation is the
// IPv6 counterpart of TestClusterObserveLateInitializesIpv4AddrsAfterCidrAllocation:
// once an ipv6Cidr-allocated HostRecord is observed, the WAPI-assigned
// address is captured back into spec.ipv6Addrs.
func TestClusterObserveLateInitializesIpv6AddrsAfterCidrAllocation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		Ipv6Addrs:   []ibclient.HostRecordIpv6Addr{{Ipv6Addr: stringPtr("2001:db8::99")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
	cr.Spec.ForProvider.Ipv6Cidr = stringPtr("2001:db8::/64")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true, got false")
	}
	if len(cr.Spec.ForProvider.Ipv6Addrs) != 1 || cr.Spec.ForProvider.Ipv6Addrs[0].Ipv6Addr != "2001:db8::99" {
		t.Errorf("Observe: spec.ipv6Addrs = %v, want [2001:db8::99]", cr.Spec.ForProvider.Ipv6Addrs)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true once ipv6Addrs is late-initialized, got false")
	}
}

// TestClusterObserveIgnoresCreateOnlyAllocationFieldsInIsUpToDate proves
// that ipv4Cidr/ipv6Cidr/filterParams/ipAddressType — create-time-only
// allocation parameters WAPI never echoes back — never factor into
// ResourceUpToDate, since they have no observed counterpart to compare
// against.
func TestClusterObserveIgnoresCreateOnlyAllocationFieldsInIsUpToDate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
	cr.Spec.ForProvider.Ipv4Cidr = stringPtr("10.0.0.0/24")
	cr.Spec.ForProvider.Ipv6Cidr = stringPtr("2001:db8::/64")
	cr.Spec.ForProvider.FilterParams = map[string]string{"*Site": "HQ"}
	cr.Spec.ForProvider.IpAddressType = stringPtr("IPV4")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true — cidr/filterParams/ipAddressType are create-time-only and must not affect the comparison, got false")
	}
	ap := cr.Status.AtProvider
	if ap.Ipv4Cidr == nil || *ap.Ipv4Cidr != "10.0.0.0/24" {
		t.Errorf("AtProvider.Ipv4Cidr = %v, want it echoed from spec (10.0.0.0/24)", ap.Ipv4Cidr)
	}
	if ap.FilterParams["*Site"] != "HQ" {
		t.Errorf("AtProvider.FilterParams = %v, want it echoed from spec", ap.FilterParams)
	}
}

// ── dynamic IP allocation: EA-filter-based (Path 2 — AllocateNextAvailableIp) ─

// newFilterAllocationServer emulates the WAPI endpoints exercised by the
// EA-filter-based next-available-IP allocation path: POST to record:host
// with an IpNextAvailable-shaped body (captured into capturedBody for
// assertions) returns allocRef, and a subsequent GET for allocRef returns
// allocatedRec — simulating a real Grid Manager resolving the
// next_available_ip function server-side and returning the finished
// HostRecord. This cannot reuse mockWapiServer's POST handler, which
// decodes the request body as a plain ibclient.HostRecord — a shape that
// does not match the nested _object_function envelope this allocation
// path sends.
func newFilterAllocationServer(t *testing.T, allocRef string, allocatedRec *ibclient.HostRecord, capturedBody *ibclient.IpNextAvailable) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	// The identity-prerequisite probe (ensureIdentityPrerequisite) runs
	// unconditionally before every Create — report the definition as
	// already present so this narrowly-scoped allocation-path test never
	// exercises that unrelated code path.
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		name := identity.EAKey
		writeJSON(w, http.StatusOK, []ibclient.EADefinition{{Name: &name}})
	})
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/record:host", func(w http.ResponseWriter, r *http.Request) {
		body, err := readAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(body, capturedBody); err != nil {
			t.Fatalf("cannot decode captured IpNextAvailable request body: %v", err)
		}
		writeJSON(w, http.StatusOK, allocRef)
	})
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("ref") != allocRef {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, allocatedRec)
	})
	return httptest.NewServer(mux)
}

// TestClusterCreateWithFilterParams verifies that a HostRecord with
// filterParams + ipAddressType set dispatches to AllocateNextAvailableIp
// instead of CreateHostRecord, sends the EA filter (plus network_view) as
// the WAPI search filter, and captures the server-assigned ref as the
// external name.
func TestClusterCreateWithFilterParams(t *testing.T) {
	allocRef := "record:host/alloc123:host.example.com/default"
	allocatedRec := &ibclient.HostRecord{
		Ref:         allocRef,
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.5.10")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	}
	var captured ibclient.IpNextAvailable
	srv := newFilterAllocationServer(t, allocRef, allocatedRec, &captured)
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
	cr.Spec.ForProvider.Ipv4Addrs = nil
	cr.Spec.ForProvider.FilterParams = map[string]string{"*Site": "HQ"}
	cr.Spec.ForProvider.IpAddressType = stringPtr("IPV4")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	if got := meta.GetExternalName(cr); got != allocRef {
		t.Errorf("Create: external-name = %q, want %q", got, allocRef)
	}
	if captured.Name != "host.example.com" {
		t.Errorf("captured request Name = %q, want host.example.com", captured.Name)
	}
	if len(captured.NextAvailableIPv4Addrs) != 1 {
		t.Fatalf("captured request NextAvailableIPv4Addrs count = %d, want 1", len(captured.NextAvailableIPv4Addrs))
	}
	info := captured.NextAvailableIPv4Addrs[0].NextavailableIPv4Addr
	if info.Function != "next_available_ip" {
		t.Errorf("captured request _object_function = %q, want next_available_ip", info.Function)
	}
	if info.ObjectParams["*Site"] != "HQ" {
		t.Errorf("captured request _object_parameters[*Site] = %q, want HQ", info.ObjectParams["*Site"])
	}
	if info.ObjectParams["network_view"] != "default" {
		t.Errorf("captured request _object_parameters[network_view] = %q, want default", info.ObjectParams["network_view"])
	}
}

// TestClusterCreateRejectsFilterParamsWithoutIpAddressType verifies
// Create surfaces the validation error (wrapped) when filterParams is set
// without ipAddressType.
func TestClusterCreateRejectsFilterParamsWithoutIpAddressType(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
	cr.Spec.ForProvider.Ipv4Addrs = nil
	cr.Spec.ForProvider.FilterParams = map[string]string{"*Site": "HQ"}

	_, err := e.Create(context.Background(), cr)
	if err == nil || !strings.Contains(err.Error(), errFilterParamsRequiresType) {
		t.Errorf("Create: err = %v, want it to contain %q", err, errFilterParamsRequiresType)
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

func TestNewHostRecordClientWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newHostRecordClientWithScheme must not hardcode
	// SslVerify to "true" — it must honor the sslVerify parameter. Both branches
	// must construct successfully (transport config validation happens
	// locally; no network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t"}
			client, err := newHostRecordClientWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newHostRecordClientWithScheme: unexpected error: %v", err)
			}
			if client == nil {
				t.Fatal("newHostRecordClientWithScheme: expected non-nil client")
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
	var comment, view *string
	var ttl *uint32
	var useTTL, configureForDNS, disable *bool
	extAttrs := map[string]string(nil)
	var aliases []string
	var ipv4Addrs []ipv4AddrValue
	var ipv6Addrs []ipv6AddrValue

	rec := &ibclient.HostRecord{
		Ea: identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	}

	changed := lateInitialize(&comment, &ttl, &useTTL, &extAttrs, &view, &configureForDNS, &disable, &aliases, &ipv4Addrs, &ipv6Addrs, rec)
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
	rec := &ibclient.HostRecord{
		Name: stringPtr("host.example.com"),
		Ea:   identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	}

	p := hostRecordCompareFields{
		Name:     stringPtr("host.example.com"),
		ExtAttrs: map[string]string{"env": "prod"},
	}

	got := isUpToDate(p, rec)
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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; got != testUIDCluster {
		t.Errorf("AtProvider.ExtAttrs[%q] = %q, want %q (full Grid EA mirror, stamp included)", identity.EAKey, got, testUIDCluster)
	}
}

// ── Identity: empty-uid refusal ──────────────────────────────────────────

func TestCreateHostRecordRefusesEmptyUID(t *testing.T) {
	p := hostRecordCompareFields{
		Name:      stringPtr("host.example.com"),
		View:      stringPtr("default"),
		Ipv4Addrs: []ipv4AddrValue{{Ipv4Addr: "10.0.0.1"}},
	}
	_, err := createHostRecord(nil, p, stringPtr("default"), nil, nil, "")
	if err == nil {
		t.Fatal("createHostRecord: expected an error for an empty uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("createHostRecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateHostRecordRefusesEmptyUID(t *testing.T) {
	p := hostRecordCompareFields{
		Name:      stringPtr("host.example.com"),
		View:      stringPtr("default"),
		Ipv4Addrs: []ipv4AddrValue{{Ipv4Addr: "10.0.0.1"}},
	}
	_, err := updateHostRecord(nil, "record:host/test1:host.example.com/default", p, "")
	if err == nil {
		t.Fatal("updateHostRecord: expected an error for an empty uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("updateHostRecord: error = %v, want it to mention the empty uid", err)
	}
}

// TestCreateHostRecordRefusesWhitespaceUID,
// TestAllocateNextAvailableHostRecordRefusesWhitespaceUID and
// TestUpdateHostRecordRefusesWhitespaceUID: a whitespace-only uid is not
// empty by a literal "" comparison, but it is not a usable identity
// either — the guard must trim before checking, matching the shared
// identity resolution ladder's own TrimSpace check.

func TestCreateHostRecordRefusesWhitespaceUID(t *testing.T) {
	p := hostRecordCompareFields{
		Name:      stringPtr("host.example.com"),
		View:      stringPtr("default"),
		Ipv4Addrs: []ipv4AddrValue{{Ipv4Addr: "10.0.0.1"}},
	}
	_, err := createHostRecord(nil, p, stringPtr("default"), nil, nil, "   ")
	if err == nil {
		t.Fatal("createHostRecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("createHostRecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestAllocateNextAvailableHostRecordRefusesWhitespaceUID(t *testing.T) {
	p := hostRecordCompareFields{
		Name: stringPtr("host.example.com"),
		View: stringPtr("default"),
	}
	_, err := allocateNextAvailableHostRecord(nil, p, stringPtr("default"), map[string]string{"*key": "val"}, "IPV4", "   ")
	if err == nil {
		t.Fatal("allocateNextAvailableHostRecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("allocateNextAvailableHostRecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateHostRecordRefusesWhitespaceUID(t *testing.T) {
	p := hostRecordCompareFields{
		Name:      stringPtr("host.example.com"),
		View:      stringPtr("default"),
		Ipv4Addrs: []ipv4AddrValue{{Ipv4Addr: "10.0.0.1"}},
	}
	_, err := updateHostRecord(nil, "record:host/test1:host.example.com/default", p, "   ")
	if err == nil {
		t.Fatal("updateHostRecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("updateHostRecord: error = %v, want it to mention the empty uid", err)
	}
}

// ── identity ladder: every remaining row, both scopes ───────────────────
//
// The tests above already prove Rotated, NotFound and the cluster-side
// HandleReuseError row. What follows fills the remaining rows the pilot
// (recorda) covers: Adopted, FoundByUID, AmbiguousMatchError, and the
// namespaced HandleReuseError row.

func hostRecordFixture(uid string) *ibclient.HostRecord {
	return &ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, uid),
	}
}

func TestClusterObserveAdoptsUnstampedObjectAndForcesUpdate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		// No Ea at all — the object has never been stamped.
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)

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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")

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

func TestClusterObserveEmptyExternalNameRecoversSingleMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foundRef := m.seed(hostRecordFixture(testUIDCluster))

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
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

	foundRef := m.seed(hostRecordFixture(testUIDNamespaced))

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "", "ProviderConfig")
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

	m.seed(&ibclient.HostRecord{Name: stringPtr("host-a.example.com"), Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}}, NetworkView: "default", View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.HostRecord{Name: stringPtr("host-b.example.com"), Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.2")}}, NetworkView: "default", View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/stale-ref:host.example.com/default")

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

	m.seed(&ibclient.HostRecord{Name: stringPtr("host-a.example.com"), Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}}, NetworkView: "default", View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})
	m.seed(&ibclient.HostRecord{Name: stringPtr("host-b.example.com"), Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.2")}}, NetworkView: "default", View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/stale-ref:host.example.com/default", "ProviderConfig")

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

	refA := m.seed(&ibclient.HostRecord{Name: stringPtr("host-a.example.com"), Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}}, NetworkView: "default", View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})
	refB := m.seed(&ibclient.HostRecord{Name: stringPtr("host-b.example.com"), Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.2")}}, NetworkView: "default", View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/stale-ref:host.example.com/default")

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

	refA := m.seed(&ibclient.HostRecord{Name: stringPtr("host-a.example.com"), Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}}, NetworkView: "default", View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})
	refB := m.seed(&ibclient.HostRecord{Name: stringPtr("host-b.example.com"), Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.2")}}, NetworkView: "default", View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDNamespaced)})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/stale-ref:host.example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		// No Ea at all — never stamped.
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)

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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		// No Ea at all — never stamped.
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")

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

func TestClusterCreateStampsIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")

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
		t.Errorf("Create: POST /record:host calls = %d, want exactly 1", createCalls)
	}
	if putCalls != 0 {
		t.Errorf("Create: PUT calls = %d, want 0 — the identity stamp must land in the same request that creates the object, no follow-up PUT", putCalls)
	}
}

func TestNamespacedCreateStampsIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "", "ProviderConfig")

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
		t.Errorf("Create: POST /record:host calls = %d, want exactly 1", createCalls)
	}
	if putCalls != 0 {
		t.Errorf("Create: PUT calls = %d, want 0 — the identity stamp must land in the same request that creates the object, no follow-up PUT", putCalls)
	}
}

// TestCreateHostRecordRefusesEmptyUIDIssuesNoMutatingCall is the
// controller-level (not just the bare-function) companion of
// TestCreateHostRecordRefusesEmptyUID: proves the httptest server records
// zero mutating requests when Create is refused for an empty uid.
func TestCreateHostRecordRefusesEmptyUIDIssuesNoMutatingCall(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "")
	cr.SetUID("")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected an error for a blank metadata.uid, got nil")
	}

	m.mu.Lock()
	createCalls, eaDefCreateCalls := m.createCalls, m.eaDefCreateCalls
	m.mu.Unlock()
	if createCalls != 0 {
		t.Errorf("Create: POST /record:host calls = %d, want 0 for a refused create", createCalls)
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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", ref)
	// Change only extAttrs — this package's mock only rotates the _ref
	// when m.renameRefOnUpdate is explicitly set (see the round-trip
	// persistence tests below), so no ambiguity here either way.
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

	ref := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(ibclient.EA{"env": "prod"}, testUIDNamespaced),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")
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

func TestClusterUpdateRefreshedExternalNamePersistsAcrossReGet(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	})
	m.renameRefOnUpdate = "record:host/renamed:renamed.example.com/default"

	cr := newClusterHostRecord("my-hostrecord", oldRef)
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()

	e := &clusterExternal{kube: kube, client: newTestClient(t, srv)}
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating rename, want a refreshed _ref")
	}

	fetched := &clusterv1alpha1.HostRecord{}
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

	oldRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
	})
	m.renameRefOnUpdate = "record:host/renamed:renamed.example.com/default"

	cr := newNamespacedHostRecord("default", "my-hostrecord", oldRef, "ProviderConfig")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()

	e := &namespacedExternal{kube: kube, client: newTestClient(t, srv)}
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating rename, want a refreshed _ref")
	}

	fetched := &namespacedv1alpha1.HostRecord{}
	if err := kube.Get(context.Background(), client.ObjectKey{Name: cr.GetName(), Namespace: cr.GetNamespace()}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Update: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

// ── identity prerequisite probe: fires only on the search failure it can
//    actually diagnose (Observe + Delete, cluster + namespaced) ─────────

func TestClusterObserveSurfacesPrerequisiteErrorFromIdentitySearch(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = errBodyEADefUnprivileged
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	c := newTestClient(t, srv)
	c.endpoint = "grid-observe-undefined-ea"
	e := &clusterExternal{kube: &recordingKubeClient{}, client: c}
	cr := newClusterHostRecord("my-hostrecord", "")

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

	ref := m.seed(hostRecordFixture(testUIDCluster))

	c := newTestClient(t, srv)
	c.endpoint = "grid-steady-state"
	e := &clusterExternal{kube: &recordingKubeClient{}, client: c}
	cr := newClusterHostRecord("my-hostrecord", ref)

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

	foreignRef := m.seed(&ibclient.HostRecord{
		Name:        stringPtr("host.example.com"),
		Ipv4Addrs:   []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}},
		NetworkView: "default",
		View:        stringPtr("default"),
		Ea:          identity.Stamp(nil, "someone-elses-uid"),
	})

	c := newTestClient(t, srv)
	c.endpoint = "grid-foreign-identity"
	e := &clusterExternal{kube: &recordingKubeClient{}, client: c}
	cr := newClusterHostRecord("my-hostrecord", foreignRef)

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

	c := newTestClient(t, srv)
	c.endpoint = "grid-ref-get-failure"
	e := &clusterExternal{kube: &recordingKubeClient{}, client: c}
	cr := newClusterHostRecord("my-hostrecord", "record:host/stale-ref:host.example.com/default")

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

	m.seed(&ibclient.HostRecord{Name: stringPtr("host-a.example.com"), Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.1")}}, NetworkView: "default", View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.HostRecord{Name: stringPtr("host-b.example.com"), Ipv4Addrs: []ibclient.HostRecordIpv4Addr{{Ipv4Addr: stringPtr("10.0.0.2")}}, NetworkView: "default", View: stringPtr("default"), Ea: identity.Stamp(nil, testUIDCluster)})

	c := newTestClient(t, srv)
	c.endpoint = "grid-ambiguous"
	e := &clusterExternal{kube: &recordingKubeClient{}, client: c}
	cr := newClusterHostRecord("my-hostrecord", "record:host/stale-ref:host.example.com/default")

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

	c := newTestClient(t, srv)
	c.endpoint = "grid-delete-undefined-ea"
	e := &clusterExternal{kube: &recordingKubeClient{}, client: c}
	cr := newClusterHostRecord("my-hostrecord", "")

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

	ref := m.seed(hostRecordFixture(testUIDCluster))

	c := newTestClient(t, srv)
	c.endpoint = "grid-delete-steady-state"
	e := &clusterExternal{kube: &recordingKubeClient{}, client: c}
	cr := newClusterHostRecord("my-hostrecord", ref)

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

	c := newTestClient(t, srv)
	c.endpoint = "grid-ns-observe-undefined-ea"
	e := &namespacedExternal{kube: &recordingKubeClient{}, client: c}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "", "ProviderConfig")

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

	ref := m.seed(hostRecordFixture(testUIDNamespaced))

	c := newTestClient(t, srv)
	c.endpoint = "grid-ns-steady-state"
	e := &namespacedExternal{kube: &recordingKubeClient{}, client: c}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")

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

	c := newTestClient(t, srv)
	c.endpoint = "grid-ns-delete-undefined-ea"
	e := &namespacedExternal{kube: &recordingKubeClient{}, client: c}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "", "ProviderConfig")

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

	ref := m.seed(hostRecordFixture(testUIDNamespaced))

	c := newTestClient(t, srv)
	c.endpoint = "grid-ns-delete-steady-state"
	e := &namespacedExternal{kube: &recordingKubeClient{}, client: c}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")

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
