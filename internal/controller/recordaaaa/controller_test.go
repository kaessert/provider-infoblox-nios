// Package recordaaaa unit tests for the AAAARecord MR controllers. Tests
// use inline httptest.NewServer mocks that emulate the WAPI record:aaaa
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package recordaaaa

import (
	"context"
	"encoding/json"
	"net"
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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordaaaa/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordaaaa/v1alpha1"
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
// newClusterAAAARecord/newNamespacedAAAARecord stamp onto their fixture
// CRs. Tests that seed a WAPI record already carrying the provider's
// identity extensible attribute (identity.Stamp) use these constants so
// the fixture's stamped uid matches the CR's own uid — the identity
// ladder's "steady state" (identity.OutcomeResolved) — unless a test is
// specifically exercising adoption, rotation, or a foreign-owned object.
const (
	testUIDCluster    = "test-uid-cluster"
	testUIDNamespaced = "test-uid-namespaced"
)

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

// newClusterAAAARecord builds a minimal cluster-scoped AAAARecord CR.
// When externalName is empty, the external-name annotation is left
// unset. When it equals crName it simulates the framework's
// NameAsExternalName initializer (the pre-create state); any other value
// simulates a Create()-assigned server ref.
func newClusterAAAARecord(crName, externalName string) *clusterv1alpha1.AAAARecord {
	cr := &clusterv1alpha1.AAAARecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.AAAARecordSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.AAAARecordParameters{
				Name:     stringPtr("host.example.com"),
				IPv6Addr: stringPtr("2001:db8::1"),
				View:     stringPtr("default"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedAAAARecord is the namespaced variant of
// newClusterAAAARecord.
func newNamespacedAAAARecord(ns, crName, externalName, pcKind string) *namespacedv1alpha1.AAAARecord {
	cr := &namespacedv1alpha1.AAAARecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
		Spec: namespacedv1alpha1.AAAARecordSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.AAAARecordParameters{
				Name:     stringPtr("host.example.com"),
				IPv6Addr: stringPtr("2001:db8::1"),
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
// mockWapiServer emulates the subset of NIOS WAPI record:aaaa endpoints
// exercised by the AAAARecord controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.RecordAAAA type so the wire format (including the EA
// {"value": ...} envelope) exactly matches what the SDK sends and expects.
//
// The PUT handler simulates the UNSTABLE _ref behavior documented for
// this resource: when the incoming name or ipv6addr differs from the
// stored record, the record is re-keyed under a freshly minted _ref (and
// the old _ref starts 404ing), mirroring live Grid Manager behavior.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.RecordAAAA
	nextRef int

	// searchCalls counts requests to the search endpoint (a GET with no
	// _ref path segment) — used to prove the identity ladder actually
	// issued a round trip rather than short-circuiting.
	searchCalls int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte

	// lastCreateIpv6Addr captures the ipv6addr value exactly as sent by
	// the controller on the most recent POST (create) request, before
	// the next-available-IP allocation simulation below replaces it
	// with a synthesized concrete address. This lets cidr/networkView
	// tests assert what was actually requested independently of what
	// address got allocated.
	lastCreateIpv6Addr *string

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
	// independent of searchCalls (record:aaaa) above.
	eaDefSearchCalls int
	eaDefCreateCalls int

	// undefinedEASearch simulates a Grid where the identity extensible
	// attribute definition itself does not exist: a GET /record:aaaa
	// search filtered by "*<EA name>" returns HTTP 400
	// ("AdmConProtoError: Unknown extensible attribute: ..."), instead
	// of the ordinary empty-array "no matches" response. Only the
	// identity-EA search path (a filter key prefixed with "*") is
	// affected.
	undefinedEASearch bool
}

// eaDefCreateBody is populated to simulate a credential that cannot
// create the identity extensible attribute definition (401/403) when a
// test sets m.eaDefCreateStatus.

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{
		records: map[string]*ibclient.RecordAAAA{},
		// The identity EA definition is present by default so every
		// pre-existing Create test sees the prerequisite as already
		// satisfied and never exercises the create-definition path.
		eaDefExists: true,
	}
}

func (m *mockWapiServer) seed(rec *ibclient.RecordAAAA) string {
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

func (m *mockWapiServer) newRefLocked(rec *ibclient.RecordAAAA) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	return "record:aaaa/test" + itoa(m.nextRef) + ":" + name + "/" + rec.View
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

// handler returns an http.Handler implementing the record:aaaa WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/record:aaaa", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.RecordAAAA
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		m.lastCreateIpv6Addr = rec.Ipv6Addr
		m.mu.Unlock()
		// Simulate the Grid Manager's dynamic-allocation behavior: when
		// the caller requested a next-available-IP (the SDK encodes
		// this as a "func:nextavailableip:<cidr>,<netview>" string in
		// the ipv6addr field), replace it with a synthesized concrete
		// address from within the requested CIDR — a real WAPI never
		// echoes the func-string back, it always resolves it to the
		// address it allocated.
		if allocated, ok := allocateFromCidr(strOrEmpty(rec.Ipv6Addr)); ok {
			rec.Ipv6Addr = &allocated
		}
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

	// Search endpoint (GetAAAARecord, and the identity ladder's EA
	// search): a GET with no _ref path segment, filtered by
	// view/name/ipv6addr query params and/or a "*<EA name>"
	// extensible-attribute filter (the syntax identity.Resolve's
	// searchByUID uses). Registered as an exact literal path so Go's
	// ServeMux prefers it over the {ref...} wildcard below for requests
	// to precisely "record:aaaa" (real _refs always carry additional
	// path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/record:aaaa", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.searchCalls++
		m.mu.Unlock()

		q := r.URL.Query()
		view := q.Get("view")
		name := q.Get("name")
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
		var matches []ibclient.RecordAAAA
		for _, rec := range m.records {
			if view != "" && rec.View != view {
				continue
			}
			if name != "" && (rec.Name == nil || *rec.Name != name) {
				continue
			}
			if ipv6addr != "" && (rec.Ipv6Addr == nil || *rec.Ipv6Addr != ipv6addr) {
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
		var incoming ibclient.RecordAAAA
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body

		nameChanged := incoming.Name != nil && (existing.Name == nil || *existing.Name != *incoming.Name)
		addrChanged := incoming.Ipv6Addr != nil && (existing.Ipv6Addr == nil || *existing.Ipv6Addr != *incoming.Ipv6Addr)

		existing.Name = incoming.Name
		existing.Ipv6Addr = incoming.Ipv6Addr
		existing.Comment = incoming.Comment
		existing.Ttl = incoming.Ttl
		existing.UseTtl = incoming.UseTtl
		existing.Ea = incoming.Ea
		existing.Zone = zoneFromName(existing.Name)

		respRef := ref
		if nameChanged || addrChanged {
			// UNSTABLE _ref: re-key the record under a freshly minted
			// _ref and retire the old one, mirroring live Grid Manager
			// behavior when name or ipv6addr changes.
			delete(m.records, ref)
			m.nextRef++
			newRef := m.newRefLocked(existing)
			existing.Ref = newRef
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

// allocateFromCidr simulates the WAPI's next-available-IP allocation:
// given the func-string address ("func:nextavailableip:<cidr>[,<netview>]")
// the SDK sends when the caller set cidr instead of a static address, it
// returns a synthesized concrete address from within that CIDR. ok is
// false when addr is not a next-available-IP func-string (e.g. a static
// address, or empty). This lets Create+Observe round-trip tests assert
// the allocated address surfaced in AtProvider differs from the
// func-string the controller sent — mirroring how a real Grid Manager
// resolves the request to a concrete IP before ever echoing it back.
func allocateFromCidr(addr string) (string, bool) {
	const prefix = "func:nextavailableip:"
	if !strings.HasPrefix(addr, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(addr, prefix)
	cidr := rest
	if idx := strings.Index(rest, ","); idx >= 0 {
		cidr = rest[:idx]
	}
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", false
	}
	ip := append(net.IP{}, ipNet.IP...)
	// Offset from the network address by a fixed amount — arbitrary, but
	// deterministic and guaranteed to differ from the caller-supplied
	// func-string without needing full allocation-tracking bookkeeping.
	ip[len(ip)-1] += 10
	return ip.String(), true
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

// newTestObjectManager builds an identity.ManagerAndConnector pointed at
// the given httptest.Server via plain HTTP (no TLS needed — the
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

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
		Comment:  stringPtr("hello"),
		Ttl:      func() *uint32 { v := uint32(300); return &v }(),
		UseTtl:   boolPtr(true),
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "record:aaaa/does-not-exist:host.example.com/default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestObservePreCreateState verifies that Observe no longer
// short-circuits on the pre-create state (external-name == CR name):
// per the identity ladder (ADR-IN-0006 §3), it maps that state to "" and
// runs one identity-EA search before concluding ResourceExists:false —
// closing the create-crash-window recovery path. The extra WAPI call
// returning zero matches is accepted and must not be optimized away.
func TestObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "") // external-name unset
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
		t.Error("Observe: want the identity ladder to search by uid even in the pre-create state (ADR-IN-0006 §3), got zero search calls")
	}
}

func TestClusterObserveServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "record:aaaa/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "record:aaaa/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map) must not panic and must produce a valid observation with
// nil-safe AtProvider fields. observeFromRecordAAAA copies optional
// pointer fields directly (never dereferences without a nil guard), so
// this test also pins that contract for future edits.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// Seed a completely bare RecordAAAA — only the SDK-assigned _ref (via
	// seed()) identifies the object. Name/View are the Go zero value
	// (nil/empty string), so zoneFromName leaves Zone at "" too.
	ref := m.seed(&ibclient.RecordAAAA{})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)

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
	if ap.IPv6Addr != nil {
		t.Errorf("AtProvider.IPv6Addr = %v, want nil", ap.IPv6Addr)
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
	// cidr/networkView are unset here (zero value) and ipv6Addr is
	// static — this also serves as the regression guard proving the
	// cidr next-available-IP path (added below) did not change the
	// pre-existing static-address Create behavior.
	cr := newClusterAAAARecord("my-aaaarecord", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// ── cluster: Create with cidr/networkView (next-available-IP) ──────────

func TestClusterCreateWithCidrAllocatesNextAvailableIP(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "")
	cr.Spec.ForProvider.IPv6Addr = nil
	cr.Spec.ForProvider.Cidr = stringPtr("2001:db8::/64")
	cr.Spec.ForProvider.NetworkView = stringPtr("my-view")

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	// cidr and networkView must be forwarded to the SDK Create call as a
	// single next-available-IP func-string.
	m.mu.Lock()
	sent := m.lastCreateIpv6Addr
	m.mu.Unlock()
	wantSent := "func:nextavailableip:2001:db8::/64,my-view"
	if sent == nil || *sent != wantSent {
		t.Errorf("Create: sent ipv6addr = %v, want %q", sent, wantSent)
	}

	// Create succeeds and the allocated IP (as resolved by the WAPI, not
	// the func-string the controller sent) appears in AtProvider once
	// Observe runs.
	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	gotIP := cr.Status.AtProvider.IPv6Addr
	if gotIP == nil || *gotIP == wantSent || *gotIP == "" {
		t.Errorf("AtProvider.IPv6Addr = %v, want a concrete allocated address distinct from the func-string", gotIP)
	}
	if gotIP != nil && *gotIP != "2001:db8::a" {
		t.Errorf("AtProvider.IPv6Addr = %q, want the mock-allocated address %q", *gotIP, "2001:db8::a")
	}
}

func TestClusterCreateWithCidrDefaultsNetworkView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "")
	cr.Spec.ForProvider.IPv6Addr = nil
	cr.Spec.ForProvider.Cidr = stringPtr("2001:db8::/64")
	cr.Spec.ForProvider.NetworkView = nil

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	sent := m.lastCreateIpv6Addr
	m.mu.Unlock()
	want := "func:nextavailableip:2001:db8::/64,default"
	if sent == nil || *sent != want {
		t.Errorf("Create: sent ipv6addr = %v, want %q", sent, want)
	}
}

func TestClusterCreateCidrAndIPv6AddrMutuallyExclusive(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "")
	cr.Spec.ForProvider.IPv6Addr = stringPtr("2001:db8::1")
	cr.Spec.ForProvider.Cidr = stringPtr("2001:db8::/64")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected an error when cidr and ipv6Addr are both set, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("Create: error = %v, want it to mention 'mutually exclusive'", err)
	}

	m.mu.Lock()
	n := len(m.records)
	m.mu.Unlock()
	if n != 0 {
		t.Errorf("Create: expected no record to be created, found %d", n)
	}
}

// TestCreateAAAARecordRejectsCidrWithStaticIP is a white-box test of the
// shared createAAAARecord wrapper: the mutual-exclusivity check must run
// before any SDK/network call is attempted (passing a nil objMgr proves
// this — a real call would panic on a nil receiver).
func TestCreateAAAARecordRejectsCidrWithStaticIP(t *testing.T) {
	_, err := createAAAARecord(nil, stringPtr("host.example.com"), stringPtr("default"), stringPtr("2001:db8::1"), nil, nil, nil, nil, stringPtr("2001:db8::/64"), nil, testUIDCluster)
	if err == nil {
		t.Fatal("createAAAARecord: expected an error when cidr and ipv6Addr are both set, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("createAAAARecord: error = %v, want it to mention 'mutually exclusive'", err)
	}
}

func TestClusterObserveMirrorsCidrAndNetworkView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)
	cr.Spec.ForProvider.Cidr = stringPtr("2001:db8::/64")
	cr.Spec.ForProvider.NetworkView = stringPtr("my-view")

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	ap := cr.Status.AtProvider
	if ap.Cidr == nil || *ap.Cidr != "2001:db8::/64" {
		t.Errorf("AtProvider.Cidr = %v, want %q", ap.Cidr, "2001:db8::/64")
	}
	if ap.NetworkView == nil || *ap.NetworkView != "my-view" {
		t.Errorf("AtProvider.NetworkView = %v, want %q", ap.NetworkView, "my-view")
	}
}

func TestClusterObserveIsUpToDateIgnoresCidrAndNetworkView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)
	cr.Spec.ForProvider.Cidr = stringPtr("2001:db8::/64")
	cr.Spec.ForProvider.NetworkView = stringPtr("my-view")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite cidr/networkView being set in spec, got false")
	}
}

// TestClusterCreateServerError verifies that a 5xx response from the WAPI
// is propagated (wrapped, not swallowed) and that no external-name is
// assigned. A totally unresponsive Grid fails at the identity-prerequisite
// probe stage (issued before the create call itself), which is still
// error-wrapped context rather than a swallowed failure.
func TestClusterCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, "identity extensible attribute definition prerequisite") {
		t.Errorf("Create: error = %q, want it to contain the prerequisite-probe context (wrapped, not swallowed)", got)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want unset after failed create", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "original-view",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)
	// Mutate the immutable view field in spec — this must NOT affect
	// ResourceUpToDate, since view is excluded from isUpToDate (WAPI has
	// no UpdateAAAARecord parameter for it).
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

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
		Comment:  stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)
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

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)

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

// TestClusterUpdateRefChangesOnNameChange pins the UNSTABLE _ref
// contract: when name changes, the WAPI response carries a new _ref and
// the controller MUST update the external-name annotation to the new
// value so subsequent reconciles use the live reference.
func TestClusterUpdateRefChangesOnNameChange(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == "" || newRef == ref {
		t.Errorf("Update: external-name annotation not refreshed after _ref change, got %q (old ref %q)", newRef, ref)
	}

	m.mu.Lock()
	_, oldStillPresent := m.records[ref]
	_, newPresent := m.records[newRef]
	m.mu.Unlock()
	if oldStillPresent {
		t.Error("Update: old _ref still present in backend after rename")
	}
	if !newPresent {
		t.Error("Update: new _ref not present in backend after rename")
	}
}

// TestClusterUpdateRefChangesOnAddrChange is the ipv6Addr-driven variant
// of TestClusterUpdateRefChangesOnNameChange.
func TestClusterUpdateRefChangesOnAddrChange(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)
	cr.Spec.ForProvider.IPv6Addr = stringPtr("2001:db8::2")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == "" || newRef == ref {
		t.Errorf("Update: external-name annotation not refreshed after _ref change, got %q (old ref %q)", newRef, ref)
	}
}

// TestClusterUpdateServerError verifies that a 5xx response from the WAPI
// update endpoint is propagated (wrapped, not swallowed) rather than
// silently accepted.
func TestClusterUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "record:aaaa/test1:host.example.com/default")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateAAAARecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateAAAARecord)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{Name: stringPtr("host.example.com"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)

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
	cr := newClusterAAAARecord("my-aaaarecord", "record:aaaa/does-not-exist:host.example.com/default")

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
	cr := newClusterAAAARecord("my-aaaarecord", "record:aaaa/test1:host.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteAAAARecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteAAAARecord)
	}
}

// TestClusterDeleteRecoversRotatedRefAndDeletes verifies the identity
// ladder's rotation-recovery path on Delete: a 404 against the stored
// (stale) _ref is not proof the object is gone — the identity-EA search
// recovers it under its current _ref (because the search matches the
// CR's own uid, stamped by Create/Update) and Delete proceeds against
// that recovered reference.
func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv6Addr: stringPtr("2001:db8::1"),
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "record:aaaa/stale-ref:host.example.com/default")

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

// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// companion happy path: a 404 against the stored _ref, and an
// identity-EA search that finds nothing, means the object really is
// gone.
func TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "record:aaaa/stale-ref:host.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
	}
}

// TestClusterObserveRecoversRotatedRefAndPersistsAnnotation is the
// Observe()-side counterpart of TestClusterDeleteRecoversRotatedRefAndDeletes:
// crossplane-runtime's managed reconciler calls Observe() before Delete()
// on the deletion path, and if Observe() reports ResourceExists:false the
// reconciler never calls Delete() at all — it just clears the finalizer,
// orphaning the Grid object. The identity ladder recovers the rotated
// reference here too, and Observe must persist it via
// ResourceLateInitialized so a later reconcile does not repeat the search.
func TestClusterObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv6Addr: stringPtr("2001:db8::1"),
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", "record:aaaa/stale-ref:host.example.com/default")

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

	foreignRef := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", foreignRef)

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

	cr := newClusterAAAARecord("my-aaaarecord", "")
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

	cr := newClusterAAAARecord("my-aaaarecord", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", ref, "ProviderConfig")

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
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "record:aaaa/does-not-exist:host.example.com/default", "ProviderConfig")

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
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "", "ProviderConfig")
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
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "record:aaaa/test1:host.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "record:aaaa/test1:host.example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordAAAA{})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", ref, "ProviderConfig")

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
	if ap.IPv6Addr != nil {
		t.Errorf("AtProvider.IPv6Addr = %v, want nil", ap.IPv6Addr)
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
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateServerError verifies that a 5xx response from the
// WAPI is propagated (wrapped, not swallowed) and that no external-name
// is assigned, in the namespaced scope. A totally unresponsive Grid
// fails at the identity-prerequisite probe stage (issued before the
// create call itself).
func TestNamespacedCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, "identity extensible attribute definition prerequisite") {
		t.Errorf("Create: error = %q, want it to contain the prerequisite-probe context (wrapped, not swallowed)", got)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want unset after failed create", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", ref, "ProviderConfig")
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

// TestNamespacedUpdateServerError verifies that a 5xx response from the
// WAPI update endpoint is propagated (wrapped, not swallowed) rather than
// silently accepted, in the namespaced scope.
func TestNamespacedUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "record:aaaa/test1:host.example.com/default", "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateAAAARecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateAAAARecord)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{Name: stringPtr("host.example.com"), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "record:aaaa/does-not-exist:host.example.com/default", "ProviderConfig")

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
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "record:aaaa/test1:host.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteAAAARecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteAAAARecord)
	}
}

// TestNamespacedDeleteRecoversRotatedRefAndDeletes is the namespaced-scope
// counterpart of TestClusterDeleteRecoversRotatedRefAndDeletes.
func TestNamespacedDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv6Addr: stringPtr("2001:db8::1"),
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "record:aaaa/stale-ref:host.example.com/default", "ProviderConfig")

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

// TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// namespaced-scope counterpart of
// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch.
func TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "record:aaaa/stale-ref:host.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
}

// TestNamespacedObserveRecoversRotatedRefAndPersistsAnnotation is the
// namespaced-scope counterpart of
// TestClusterObserveRecoversRotatedRefAndPersistsAnnotation.
func TestNamespacedObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv6Addr: stringPtr("2001:db8::1"),
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "record:aaaa/stale-ref:host.example.com/default", "ProviderConfig")

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

	cr := newNamespacedAAAARecord(ns, "my-aaaarecord", "", "ProviderConfig")
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

	cr := newNamespacedAAAARecord("app-ns", "my-aaaarecord", "", "ClusterProviderConfig")
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

	cr := newNamespacedAAAARecord("default", "my-aaaarecord", "", "SomeOtherKind")
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

	rec := &ibclient.RecordAAAA{
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

	rec := &ibclient.RecordAAAA{
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
	rec := &ibclient.RecordAAAA{
		Ttl:    &zoneDefault,
		UseTtl: boolPtr(false),
	}

	lateInitialize(&comment, &ttl, &useTTL, &extAttrs, rec)

	if ttl != nil {
		t.Errorf("lateInitialize: ttl = %v, want nil (useTtl is off, observed ttl is the zone default, not a user value)", *ttl)
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that name,
// ipv6Addr, and view — the CRD's required AAAARecordParameters fields —
// are never overwritten by Observe()'s late-init step. lateInitialize
// only accepts pointers to the optional fields (comment, ttl, useTtl,
// extAttrs), so a spec/observed mismatch on a required field can never
// occur through the real WAPI flow (name+view compose the object's
// _ref) — this test drives it artificially to pin the guarantee.
func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("observed.example.com"),
		Ipv6Addr: stringPtr("2001:db8::99"),
		View:     "observed-view",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)
	cr.Spec.ForProvider.Name = stringPtr("host.example.com")
	cr.Spec.ForProvider.IPv6Addr = stringPtr("2001:db8::1")
	cr.Spec.ForProvider.View = stringPtr("default")

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := *cr.Spec.ForProvider.Name; got != "host.example.com" {
		t.Errorf("Observe: required field Name late-initialized to %q, want unchanged %q", got, "host.example.com")
	}
	if got := *cr.Spec.ForProvider.IPv6Addr; got != "2001:db8::1" {
		t.Errorf("Observe: required field IPv6Addr late-initialized to %q, want unchanged %q", got, "2001:db8::1")
	}
	if got := *cr.Spec.ForProvider.View; got != "default" {
		t.Errorf("Observe: required field View late-initialized to %q, want unchanged %q", got, "default")
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedRecord := func() *ibclient.RecordAAAA {
		ttl := uint32(300)
		return &ibclient.RecordAAAA{
			Name:     stringPtr("host.example.com"),
			Ipv6Addr: stringPtr("2001:db8::1"),
			Comment:  stringPtr("hello"),
			Ttl:      &ttl,
			UseTtl:   boolPtr(true),
			Ea:       ibclient.EA{"env": "prod"},
		}
	}

	cases := map[string]struct {
		reason   string
		name     *string
		ipv6Addr *string
		comment  *string
		ttl      *uint32
		useTTL   *bool
		extAttrs map[string]string
		want     bool
	}{
		"IdenticalFieldsAreUpToDate": {
			reason:   "when every mutable field matches the observed record, the resource must be reported up to date",
			name:     stringPtr("host.example.com"),
			ipv6Addr: stringPtr("2001:db8::1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     true,
		},
		"ChangedNameIsNotUpToDate": {
			reason:   "a changed name must be detected as drift",
			name:     stringPtr("renamed.example.com"),
			ipv6Addr: stringPtr("2001:db8::1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedIPv6AddrIsNotUpToDate": {
			reason:   "a changed ipv6Addr must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv6Addr: stringPtr("2001:db8::2"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedCommentIsNotUpToDate": {
			reason:   "a changed comment must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv6Addr: stringPtr("2001:db8::1"),
			comment:  stringPtr("goodbye"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedTTLIsNotUpToDate": {
			reason:   "a changed ttl must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv6Addr: stringPtr("2001:db8::1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(600),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedUseTTLIsNotUpToDate": {
			reason:   "a changed useTtl flag must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv6Addr: stringPtr("2001:db8::1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(false),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ExtAttrsDifferentValueIsNotUpToDate": {
			reason:   "an extAttrs value change on an existing key must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv6Addr: stringPtr("2001:db8::1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "staging"},
			want:     false,
		},
		"ExtAttrsDifferentKeyIsNotUpToDate": {
			reason:   "an extAttrs key added/removed must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv6Addr: stringPtr("2001:db8::1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"owner": "platform-team"},
			want:     false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := isUpToDate(tc.name, tc.ipv6Addr, tc.comment, tc.ttl, tc.useTTL, tc.extAttrs, observedRecord())
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
	observed := &ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		Comment:  stringPtr("hello"),
		Ttl:      &zoneDefault,
		UseTtl:   boolPtr(false),
		Ea:       ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		stringPtr("host.example.com"),
		stringPtr("2001:db8::1"),
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
	observed := &ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		Comment:  stringPtr("hello"),
		Ttl:      &ttl,
		UseTtl:   boolPtr(true),
		Ea:       ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		stringPtr("host.example.com"),
		stringPtr("2001:db8::1"),
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
	rec := &ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
	}
	// The observed record carries no extattrs (nil Ea) — a spec with an
	// explicit empty map must still compare as up to date, since
	// extAttrsEqual treats nil and empty as equivalent (avoids a phantom
	// diff when the WAPI response omits an empty extattrs object).
	got := isUpToDate(stringPtr("host.example.com"), stringPtr("2001:db8::1"), nil, nil, nil, map[string]string{}, rec)
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

// ── ttlOrZero: uint32 conversion edge cases ─────────────────────────────

func TestTtlOrZero(t *testing.T) {
	cases := map[string]struct {
		reason string
		ttl    *uint32
		want   uint32
	}{
		"NilReturnsZero": {
			reason: "an unset TTL pointer must map to 0 — the WAPI create/update calls take a plain uint32 with no separate unset sentinel",
			ttl:    nil,
			want:   0,
		},
		"ValidValuePassesThrough": {
			reason: "a set TTL must pass through unchanged",
			ttl:    uint32Ptr(300),
			want:   300,
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

// ── Delete: RemoveAssociatedPtr (documented SDK limitation) ────────────
//
// The AAAARecordParameters schema accepts removeAssociatedPtr for schema
// completeness, but the infoblox-go-client SDK's DeleteAAAARecord wrapper
// takes only the object reference — it exposes no query-parameter or
// request-body hook for the WAPI remove_associated_ptr delete option (see
// deleteAAAARecord's doc comment in controller.go). This test pins that
// documented limitation: Delete succeeds identically regardless of the
// field's value, because it is never forwarded to the SDK call.
func TestClusterDeleteIgnoresRemoveAssociatedPtr(t *testing.T) {
	cases := map[string]*bool{
		"Unset": nil,
		"True":  boolPtr(true),
		"False": boolPtr(false),
	}

	for name, removeAssociatedPtr := range cases {
		t.Run(name, func(t *testing.T) {
			m := newMockWapiServer()
			srv := httptest.NewServer(m.handler())
			defer srv.Close()

			ref := m.seed(&ibclient.RecordAAAA{Name: stringPtr("host.example.com"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

			e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
			cr := newClusterAAAARecord("my-aaaarecord", ref)
			cr.Spec.ForProvider.RemoveAssociatedPtr = removeAssociatedPtr

			if _, err := e.Delete(context.Background(), cr); err != nil {
				t.Fatalf("Delete: unexpected error: %v", err)
			}

			m.mu.Lock()
			_, stillExists := m.records[ref]
			m.mu.Unlock()
			if stillExists {
				t.Error("Delete: record still present after Delete regardless of RemoveAssociatedPtr value")
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

	rec := &ibclient.RecordAAAA{
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
	rec := &ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	}

	got := isUpToDate(stringPtr("host.example.com"), stringPtr("2001:db8::1"), nil, nil, nil, map[string]string{"env": "prod"}, rec)
	if !got {
		t.Error("isUpToDate: want true when spec.forProvider.extAttrs matches the Grid map with the identity stamp stripped, got false")
	}
}

// TestClusterObserveAtProviderExtAttrsIncludesIdentityKey proves
// status.atProvider.extAttrs mirrors the Grid's full extattrs map,
// identity stamp included — convention 0032's full-mirror AtProvider
// guarantee applies to the read-only status mirror even though the
// identity key is stripped from the spec comparison.
func TestClusterObserveAtProviderExtAttrsIncludesIdentityKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordAAAA{
		Name:     stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
		View:     "default",
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv).Manager, conn: newTestObjectManager(t, srv).Connector, prober: identity.NewProber()}
	cr := newClusterAAAARecord("my-aaaarecord", ref)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; got != testUIDCluster {
		t.Errorf("AtProvider.ExtAttrs[%q] = %q, want %q (full Grid EA mirror, stamp included)", identity.EAKey, got, testUIDCluster)
	}
}

// ── Identity: empty-uid refusal ──────────────────────────────────────────

func TestCreateAAAARecordRefusesEmptyUID(t *testing.T) {
	_, err := createAAAARecord(nil, stringPtr("host.example.com"), stringPtr("default"), stringPtr("2001:db8::1"), nil, nil, nil, nil, nil, nil, "")
	if err == nil {
		t.Fatal("createAAAARecord: expected an error for an empty uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("createAAAARecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateAAAARecordRefusesEmptyUID(t *testing.T) {
	_, err := updateAAAARecord(nil, "record:aaaa/test1:host.example.com/default", stringPtr("host.example.com"), stringPtr("2001:db8::1"), nil, nil, nil, nil, "")
	if err == nil {
		t.Fatal("updateAAAARecord: expected an error for an empty uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("updateAAAARecord: error = %v, want it to mention the empty uid", err)
	}
}

// TestCreateAAAARecordRefusesWhitespaceUID and
// TestUpdateAAAARecordRefusesWhitespaceUID: a whitespace-only uid is not
// empty by a literal "" comparison, but it is not a usable identity
// either — the guard must trim before checking, matching the shared
// identity resolution ladder's own TrimSpace check.

func TestCreateAAAARecordRefusesWhitespaceUID(t *testing.T) {
	_, err := createAAAARecord(nil, stringPtr("host.example.com"), stringPtr("default"), stringPtr("2001:db8::1"), nil, nil, nil, nil, nil, nil, "   ")
	if err == nil {
		t.Fatal("createAAAARecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("createAAAARecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateAAAARecordRefusesWhitespaceUID(t *testing.T) {
	_, err := updateAAAARecord(nil, "record:aaaa/test1:host.example.com/default", stringPtr("host.example.com"), stringPtr("2001:db8::1"), nil, nil, nil, nil, "   ")
	if err == nil {
		t.Fatal("updateAAAARecord: expected an error for a whitespace-only uid, got nil")
	}
	if !strings.Contains(err.Error(), "metadata.uid is empty") {
		t.Errorf("updateAAAARecord: error = %v, want it to mention the empty uid", err)
	}
}
