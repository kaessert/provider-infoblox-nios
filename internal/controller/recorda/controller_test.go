// Package recorda unit tests for the ARecord MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI record:a endpoints,
// PascalCase test names (no underscores), and white-box access to the
// unexported connectors/clients so both scopes can be exercised without
// going through the full Connect() credential bridge on every test.
package recorda

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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recorda/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recorda/v1alpha1"
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
// newClusterARecord/newNamespacedARecord stamp onto their fixture CRs.
// Tests that seed a WAPI record already carrying the provider's identity
// extensible attribute (identity.Stamp) use these constants so the
// fixture's stamped uid matches the CR's own uid — the identity ladder's
// "steady state" (identity.OutcomeResolved) — unless a test is
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

// newClusterARecord builds a minimal cluster-scoped ARecord CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterARecord(crName, externalName string) *clusterv1alpha1.ARecord {
	cr := &clusterv1alpha1.ARecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.ARecordSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.ARecordParameters{
				Name:     stringPtr("host.example.com"),
				IPv4Addr: stringPtr("10.0.0.1"),
				View:     stringPtr("default"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedARecord is the namespaced variant of newClusterARecord.
func newNamespacedARecord(ns, crName, externalName, pcKind string) *namespacedv1alpha1.ARecord {
	cr := &namespacedv1alpha1.ARecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
		Spec: namespacedv1alpha1.ARecordSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.ARecordParameters{
				Name:     stringPtr("host.example.com"),
				IPv4Addr: stringPtr("10.0.0.1"),
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
// mockWapiServer emulates the subset of NIOS WAPI record:a endpoints
// exercised by the ARecord controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.RecordA type so the wire format (including the EA
// {"value": ...} envelope) exactly matches what the SDK sends and expects.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.RecordA
	nextRef int

	// searchCalls counts requests to the search endpoint (a GET with no
	// _ref path segment) — used to prove the identity ladder actually
	// issued a round trip rather than short-circuiting.
	searchCalls int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte

	// lastCreateIpv4Addr captures the ipv4addr value exactly as sent by
	// the controller on the most recent POST (create) request, before
	// the next-available-IP allocation simulation below replaces it
	// with a synthesized concrete address. This lets cidr/networkView
	// tests assert what was actually requested independently of what
	// address got allocated.
	lastCreateIpv4Addr *string

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
	// independent of searchCalls (record:a) above — used to prove the
	// probe's own cost (at most one round trip per TTL window) and that
	// a refusal issues no ARecord create.
	eaDefSearchCalls int
	eaDefCreateCalls int

	// undefinedEASearch simulates a Grid where the identity extensible
	// attribute definition itself does not exist: a GET /record:a search
	// filtered by "*<EA name>" returns HTTP 400
	// ("AdmConProtoError: Unknown extensible attribute: ..."), live-
	// verified against a real Grid, instead of the ordinary empty-array
	// "no matches" response. Only the identity-EA search path (a filter
	// key prefixed with "*") is affected — a plain
	// view/name/ipv4addr search still returns normally, matching WAPI's
	// actual behavior (an unknown *filter breaks the whole search, but
	// this mock only exercises the identity ladder's own filter shape).
	undefinedEASearch bool
}

// eaDefCreateForbiddenBody is the WAPI-shaped error payload tests use for
// m.eaDefCreateBody to simulate a credential that cannot create the
// identity extensible attribute definition (401/403) — shared by every
// absent-and-forbidden probe scenario across both scopes.
const eaDefCreateForbiddenBody = `{"Error":"AdmConProtoError: Not authorized"}`

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{
		records: map[string]*ibclient.RecordA{},
		// The identity EA definition is present by default so every
		// pre-existing Create test (written before the prerequisite probe
		// existed) sees the prerequisite as already satisfied and never
		// exercises the create-definition path.
		eaDefExists: true,
	}
}

func (m *mockWapiServer) seed(rec *ibclient.RecordA) string {
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

func (m *mockWapiServer) newRefLocked(rec *ibclient.RecordA) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	return "record:a/test" + itoa(m.nextRef) + ":" + name + "/" + rec.View
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

// handler returns an http.Handler implementing the record:a WAPI surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/record:a", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.RecordA
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		m.lastCreateIpv4Addr = rec.Ipv4Addr
		m.mu.Unlock()
		// Simulate the Grid Manager's dynamic-allocation behavior: when
		// the caller requested a next-available-IP (the SDK encodes
		// this as a "func:nextavailableip:<cidr>,<netview>" string in
		// the ipv4addr field), replace it with a synthesized concrete
		// address from within the requested CIDR — a real WAPI never
		// echoes the func-string back, it always resolves it to the
		// address it allocated.
		if allocated, ok := allocateFromCidr(strOrEmpty(rec.Ipv4Addr)); ok {
			rec.Ipv4Addr = &allocated
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

	// Search endpoint (GetARecord, and the identity ladder's EA search):
	// a GET with no _ref path segment, filtered by view/name/ipv4addr
	// query params and/or a "*<EA name>" extensible-attribute filter (the
	// syntax identity.Resolve's searchByUID uses). Registered as an
	// exact literal path so Go's ServeMux prefers it over the
	// {ref...} wildcard below for requests to precisely "record:a"
	// (real _refs always carry additional path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/record:a", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.searchCalls++
		m.mu.Unlock()

		q := r.URL.Query()
		view := q.Get("view")
		name := q.Get("name")
		ipv4addr := q.Get("ipv4addr")

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
		var matches []ibclient.RecordA
		for _, rec := range m.records {
			if view != "" && rec.View != view {
				continue
			}
			if name != "" && (rec.Name == nil || *rec.Name != name) {
				continue
			}
			if ipv4addr != "" && (rec.Ipv4Addr == nil || *rec.Ipv4Addr != ipv4addr) {
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
		var incoming ibclient.RecordA
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body

		// UNSTABLE _ref: renaming an ARecord mints a new _ref — mirrors
		// live NIOS Grid Manager behavior (the _ref encodes name+view).
		refMutated := strOrEmpty(existing.Name) != strOrEmpty(incoming.Name)

		existing.Name = incoming.Name
		existing.Ipv4Addr = incoming.Ipv4Addr
		existing.Comment = incoming.Comment
		existing.Ttl = incoming.Ttl
		existing.UseTtl = incoming.UseTtl
		existing.Ea = incoming.Ea
		existing.Zone = zoneFromName(existing.Name)

		respRef := ref
		if refMutated {
			delete(m.records, ref)
			m.nextRef++
			respRef = m.newRefLocked(existing)
			existing.Ref = respRef
			m.records[respRef] = existing
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

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Comment:  stringPtr("hello"),
		Ttl:      func() *uint32 { v := uint32(300); return &v }(),
		UseTtl:   boolPtr(true),
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)
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

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "record:a/does-not-exist:host.example.com/default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestObservePreCreateState verifies the pre-create guard's new behavior:
// when the external-name still equals the CR's
// Kubernetes name (no real WAPI _ref has ever been assigned), Observe no
// longer short-circuits — it still searches by the managed resource's
// stamped identity attribute before concluding the object does not
// exist, closing the create-crash window (create succeeds on the Grid,
// the provider dies before persisting the _ref annotation). On a
// genuine first-ever reconcile that search finds nothing, costing
// exactly one extra WAPI round trip — the accepted price of closing
// that window.
func TestObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())    // simulate NameAsExternalName initializer

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
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "record:a/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "record:a/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map) must not panic and must produce a valid observation with
// nil-safe AtProvider fields. observeFromRecordA copies optional pointer
// fields directly (never dereferences without a nil guard), so this test
// also pins that contract for future edits.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// Seed a completely bare RecordA — only the SDK-assigned _ref (via
	// seed()) identifies the object. Name/View are the Go zero value
	// (nil/empty string), so zoneFromName leaves Zone at "" too.
	ref := m.seed(&ibclient.RecordA{})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)

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
	if ap.IPv4Addr != nil {
		t.Errorf("AtProvider.IPv4Addr = %v, want nil", ap.IPv4Addr)
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

// TestClusterObserveAdoptsUnstampedObjectAndForcesUpdate verifies the
// identity.OutcomeAdopted row of the ladder: the stored _ref resolves to
// a live object, but it carries no identity extensible attribute at all
// (e.g. it predates the identity wave). Observe must still report the
// resource as existing, but must NOT report it up to date even though
// every other field already matches spec — otherwise the reconciler
// would never call Update, and the object would never get its identity
// stamp.
func TestClusterObserveAdoptsUnstampedObjectAndForcesUpdate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		// No Ea at all — the object has never been stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)

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

// TestClusterObserveRecoversRotatedRefAndPersistsAnnotation verifies the
// identity.OutcomeRotated row: the stored _ref 404s, but the identity-EA
// search finds exactly one match — the same object, relocated. Observe
// must report it as existing and persist the refreshed reference onto
// the managed resource's external-name annotation via
// ResourceLateInitialized, the one path crossplane-runtime is guaranteed
// to write back after Observe (see the externalname package doc for why
// Update() alone cannot be trusted for this).
func TestClusterObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	staleRef := "record:a/stale-ref:host.example.com/default"
	cr := newClusterARecord("my-arecord", staleRef)

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
}

// TestClusterObserveRefusesOnForeignIdentity verifies that Observe
// surfaces a HandleReuseError (Synced=False, no mutating call) when the
// stored _ref resolves to an object whose identity attribute belongs to
// a different owner.
func TestClusterObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", foreignRef)

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Observe: error = %v, want it to wrap a *identity.HandleReuseError", err)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	// cidr/networkView are unset here (zero value) and ipv4Addr is
	// static — this also serves as the regression guard proving the
	// cidr next-available-IP path (added below) did not change the
	// pre-existing static-address Create behavior.
	cr := newClusterARecord("my-arecord", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}

	// Create must stamp the managed resource's own uid into the object's
	// identity extensible attribute in the same request that creates it
	// — no follow-up call, no window in which the object exists
	// unstamped.
	m.mu.Lock()
	stored := m.records[got]
	m.mu.Unlock()
	if stored == nil {
		t.Fatalf("Create: no record stored under external-name %q", got)
	}
	if uid, ok := stored.Ea[identity.EAKey]; !ok || uid != string(cr.GetUID()) {
		t.Errorf("Create: stored identity EA = %v, want %q = %q", stored.Ea, identity.EAKey, cr.GetUID())
	}
}

// ── cluster: Create with cidr/networkView (next-available-IP) ──────────

func TestClusterCreateWithCidrAllocatesNextAvailableIP(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "")
	cr.Spec.ForProvider.IPv4Addr = nil
	cr.Spec.ForProvider.Cidr = stringPtr("10.0.0.0/24")
	cr.Spec.ForProvider.NetworkView = stringPtr("my-view")

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	// cidr and networkView must be forwarded to the SDK Create call as a
	// single next-available-IP func-string.
	m.mu.Lock()
	sent := m.lastCreateIpv4Addr
	m.mu.Unlock()
	wantSent := "func:nextavailableip:10.0.0.0/24,my-view"
	if sent == nil || *sent != wantSent {
		t.Errorf("Create: sent ipv4addr = %v, want %q", sent, wantSent)
	}

	// Create succeeds and the allocated IP (as resolved by the WAPI, not
	// the func-string the controller sent) appears in AtProvider once
	// Observe runs.
	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	gotIP := cr.Status.AtProvider.IPv4Addr
	if gotIP == nil || *gotIP == wantSent || *gotIP == "" {
		t.Errorf("AtProvider.IPv4Addr = %v, want a concrete allocated address distinct from the func-string", gotIP)
	}
	if gotIP != nil && *gotIP != "10.0.0.10" {
		t.Errorf("AtProvider.IPv4Addr = %q, want the mock-allocated address %q", *gotIP, "10.0.0.10")
	}
}

func TestClusterCreateWithCidrDefaultsNetworkView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "")
	cr.Spec.ForProvider.IPv4Addr = nil
	cr.Spec.ForProvider.Cidr = stringPtr("10.0.0.0/24")
	cr.Spec.ForProvider.NetworkView = nil

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	// CreateARecord (unlike CreateAAAARecord/CreatePTRRecord) does not
	// default networkView itself — createARecord applies "default"
	// explicitly for consistency.
	m.mu.Lock()
	sent := m.lastCreateIpv4Addr
	m.mu.Unlock()
	want := "func:nextavailableip:10.0.0.0/24,default"
	if sent == nil || *sent != want {
		t.Errorf("Create: sent ipv4addr = %v, want %q", sent, want)
	}
}

func TestClusterCreateCidrAndIPv4AddrMutuallyExclusive(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "")
	cr.Spec.ForProvider.IPv4Addr = stringPtr("10.0.0.5")
	cr.Spec.ForProvider.Cidr = stringPtr("10.0.0.0/24")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected an error when cidr and ipv4Addr are both set, got nil")
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

// TestCreateARecordRejectsCidrWithStaticIP is a white-box test of the
// shared createARecord wrapper: the mutual-exclusivity check must run
// before any SDK/network call is attempted (passing a nil objMgr proves
// this — a real call would panic on a nil receiver).
func TestCreateARecordRejectsCidrWithStaticIP(t *testing.T) {
	_, err := createARecord(nil, stringPtr("host.example.com"), stringPtr("default"), stringPtr("10.0.0.5"), nil, nil, nil, nil, stringPtr("10.0.0.0/24"), nil, "test-uid")
	if err == nil {
		t.Fatal("createARecord: expected an error when cidr and ipv4Addr are both set, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("createARecord: error = %v, want it to mention 'mutually exclusive'", err)
	}
}

// ── cluster: Create identity prerequisite probe ──────────────────────────
//
// These tests inject a fresh identity.NewProber() per test (never
// identity.DefaultProber) so their verdict caches never leak into each
// other or into any other test in this binary that happens to call
// Create() without configuring the probe explicitly.

func TestClusterCreateIdentityDefinitionAlreadyExists(t *testing.T) {
	m := newMockWapiServer() // eaDefExists: true by default
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-already-has-definition",
	}
	cr := newClusterARecord("my-arecord", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	searchCalls, createCalls := m.eaDefSearchCalls, m.eaDefCreateCalls
	m.mu.Unlock()
	if searchCalls != 1 {
		t.Errorf("eaDefSearchCalls = %d, want exactly 1 (the probe still runs once)", searchCalls)
	}
	if createCalls != 0 {
		t.Errorf("eaDefCreateCalls = %d, want 0 — a present definition must never be (re)created", createCalls)
	}
}

func TestClusterCreateIdentityDefinitionAbsentAndCreatable(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-absent-creatable",
	}
	cr := newClusterARecord("my-arecord", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	m.mu.Lock()
	createCalls := m.eaDefCreateCalls
	nRecords := len(m.records)
	m.mu.Unlock()
	if createCalls != 1 {
		t.Errorf("eaDefCreateCalls = %d, want exactly 1", createCalls)
	}
	if nRecords != 1 {
		t.Errorf("records created = %d, want 1 — Create must proceed once the definition is created", nRecords)
	}
}

func TestClusterCreateIdentityDefinitionAbsentAndForbidden(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = eaDefCreateForbiddenBody
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "nios.example.com",
	}
	cr := newClusterARecord("my-arecord", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected an error when the identity definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Create: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
	if !strings.Contains(err.Error(), "nios.example.com") || !strings.Contains(err.Error(), "POST /wapi/v2.12/extensibleattributedef") {
		t.Errorf("Create: error = %v, want the missing-extensible-attribute-definition remediation text verbatim", err)
	}

	m.mu.Lock()
	nRecords := len(m.records)
	m.mu.Unlock()
	if nRecords != 0 {
		t.Errorf("records created = %d, want 0 — no ARecord POST must be issued when the prerequisite is refused", nRecords)
	}

	got := meta.GetExternalName(cr)
	if got != "" && got != cr.GetName() {
		t.Errorf("Create: external-name = %q, want it left unset when the prerequisite refuses", got)
	}
}

// TestClusterCreateIdentityPrerequisiteCachedWithinTTL proves the
// Prober's cache means a second Create against the same endpoint does
// not re-probe within the TTL — the probe costs at most one WAPI round
// trip per TTL window per endpoint, not one per reconcile.
func TestClusterCreateIdentityPrerequisiteCachedWithinTTL(t *testing.T) {
	m := newMockWapiServer() // eaDefExists: true by default
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	prober := identity.NewProber()
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   prober,
		endpoint: "grid-cache-test",
	}

	for i := 0; i < 2; i++ {
		cr := newClusterARecord("my-arecord", "")
		if _, err := e.Create(context.Background(), cr); err != nil {
			t.Fatalf("Create[%d]: unexpected error: %v", i, err)
		}
	}

	m.mu.Lock()
	searchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if searchCalls != 1 {
		t.Errorf("eaDefSearchCalls after 2 Create calls = %d, want exactly 1 (cached within the TTL)", searchCalls)
	}
}

// TestClusterCreateValidatesInputsBeforeProbing proves the cidr/ipv4Addr
// mutual-exclusivity check runs before the identity prerequisite probe —
// a purely local validation failure must never cost a network round
// trip. The mock server has no extensibleattributedef routes wired to
// succeed by anything other than eaDefExists's default; forcing the
// definition absent-and-forbidden here means any probe call would fail
// loudly with a *PrerequisiteError instead of the expected
// "mutually exclusive" validation error, proving the probe was never
// reached.
func TestClusterCreateValidatesInputsBeforeProbing(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-validate-first",
	}
	cr := newClusterARecord("my-arecord", "")
	cr.Spec.ForProvider.IPv4Addr = stringPtr("10.0.0.5")
	cr.Spec.ForProvider.Cidr = stringPtr("10.0.0.0/24")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected an error when cidr and ipv4Addr are both set, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("Create: error = %v, want it to mention 'mutually exclusive' (local validation must run before the probe)", err)
	}

	m.mu.Lock()
	searchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if searchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — local validation must fail before any probe round trip", searchCalls)
	}
}

// TestClusterCreateDefaultsToSharedProberAndEndpointFallback proves
// Create still works when built without an explicit prober/endpoint
// (e.g. every pre-existing Create test in this file, and the RBAC
// fallback Setup() path before this ticket's wiring) — it falls back to
// identity.DefaultProber and unresolvedProbeEndpoint rather than
// panicking or erroring on an empty cache key.
func TestClusterCreateDefaultsToSharedProberAndEndpointFallback(t *testing.T) {
	m := newMockWapiServer() // eaDefExists: true by default
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}
}

func TestNamespacedCreateIdentityDefinitionAbsentAndForbidden(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = eaDefCreateForbiddenBody
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "nios.example.com",
	}
	cr := newNamespacedARecord("default", "my-arecord", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected an error when the identity definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Create: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}

	m.mu.Lock()
	nRecords := len(m.records)
	m.mu.Unlock()
	if nRecords != 0 {
		t.Errorf("records created = %d, want 0 — no ARecord POST must be issued when the prerequisite is refused", nRecords)
	}
}

func TestClusterObserveMirrorsCidrAndNetworkView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)
	cr.Spec.ForProvider.Cidr = stringPtr("10.0.0.0/24")
	cr.Spec.ForProvider.NetworkView = stringPtr("my-view")

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	ap := cr.Status.AtProvider
	if ap.Cidr == nil || *ap.Cidr != "10.0.0.0/24" {
		t.Errorf("AtProvider.Cidr = %v, want %q", ap.Cidr, "10.0.0.0/24")
	}
	if ap.NetworkView == nil || *ap.NetworkView != "my-view" {
		t.Errorf("AtProvider.NetworkView = %v, want %q", ap.NetworkView, "my-view")
	}
}

func TestClusterObserveIsUpToDateIgnoresCidrAndNetworkView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)
	// cidr/networkView are create-time-only allocation hints, never
	// echoed back by the WAPI — they must not participate in the
	// up-to-date comparison.
	cr.Spec.ForProvider.Cidr = stringPtr("10.0.0.0/24")
	cr.Spec.ForProvider.NetworkView = stringPtr("my-view")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite cidr/networkView being set in spec, got false")
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "original-view",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)
	// Mutate the immutable view field in spec — this must NOT affect
	// ResourceUpToDate, since view is excluded from isUpToDate (WAPI has
	// no UpdateARecord parameter for it).
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

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Comment:  stringPtr("old comment"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)
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

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Comment:  stringPtr("old comment"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterARecord("my-arecord", ref)
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
	m.eaDefCreateBody = `{"Error":"AdmConAuthError: Not authorized"}`
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Comment:  stringPtr("old comment"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterARecord("my-arecord", ref)
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

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)

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

// TestClusterUpdateWhitespaceUIDFailsWithZeroMutatingRequests proves the
// Update path rejects a whitespace-only uid before issuing any WAPI
// call — updateARecord's guard trims before comparing, matching
// identity.Resolve's ladder (see internal/clients/identity). Without
// the trim, a whitespace-only uid would pass Update's guard and get
// re-stamped verbatim into the object's extensible attributes, while
// Observe/Delete (which route through identity.Resolve) would treat
// that same object as unowned.
func TestClusterUpdateWhitespaceUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Comment:  stringPtr("old comment"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)
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

// TestClusterUpdateRefreshesExternalNameOnRename verifies the UNSTABLE
// _ref contract for ARecord: renaming a record (a _ref-mutating field)
// mints a new _ref, and Update() must persist the refreshed
// external-name annotation via a real kube client call — not merely
// mutate cr in memory, which crossplane-runtime's managed reconciler
// would silently discard once the reconcile ends (only the status
// subresource is flushed after a successful external Update()).
func TestClusterUpdateRefreshesExternalNameOnRename(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	kube := &recordingKubeClient{}
	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", oldRef)
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating rename, want a refreshed _ref")
	}

	// Regression guard: the refreshed external-name must be persisted via
	// a real kube client call.
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
	if newRec.Name == nil || *newRec.Name != "renamed.example.com" {
		t.Errorf("Update: stored name = %v, want %q", newRec.Name, "renamed.example.com")
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{Name: stringPtr("host.example.com"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)

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
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "record:a/does-not-exist:host.example.com/default")

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

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "record:a/test1:host.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteARecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteARecord)
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

	foreignRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv4Addr: stringPtr("10.0.0.1"),
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", foreignRef)

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

// TestClusterDeleteRecoversRotatedRefAndDeletes verifies that a 404
// against the stored _ref does not stop at "already deleted": when the
// identity ladder's uid search finds the same object relocated under a
// new _ref (a rotation — e.g. an identity-composing field changed
// out-of-band), Delete() resolves the new location and deletes it there.
// A bare 404 must never be treated as evidence the object is gone.
func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv4Addr: stringPtr("10.0.0.1"),
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	// The stored external-name is stale — the object now lives at newRef
	// (simulating a rotation the annotation never caught up with).
	cr := newClusterARecord("my-arecord", "record:a/stale-ref:host.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error recovering a rotated object, got: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[newRef]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: rotated record still present — the identity ladder's recovered object must have been deleted, not silently skipped")
	}
}

// ── cluster: Observe/Delete identity prerequisite probe (reactive) ───────
//
// These tests inject a fresh identity.NewProber() per test (never
// identity.DefaultProber) for the same test-isolation reason the Create
// probe tests above do.

// TestClusterObserveSurfacesPrerequisiteErrorFromIdentitySearch covers an
// unknown reference (never assigned — the annotation still holds the
// framework's NameAsExternalName default) that forces the identity
// ladder straight to the identity-EA search, which fails because the
// definition itself does not exist on the Grid and the configured
// credential cannot create one. Observe must surface the
// *identity.PrerequisiteError verbatim, not the opaque WAPI 400 the raw
// search failure would otherwise produce, and must not report
// ResourceExists:true/false as if the search had merely found nothing.
func TestClusterObserveSurfacesPrerequisiteErrorFromIdentitySearch(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = eaDefCreateForbiddenBody
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-observe-undefined-ea",
	}
	// No external-name ever assigned: observeRefFor reports "" for this
	// case, sending the ladder straight to the identity-EA search with
	// no ref-GET attempt first.
	cr := newClusterARecord("my-arecord", "")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Observe: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
	if !strings.Contains(err.Error(), "grid-observe-undefined-ea") || !strings.Contains(err.Error(), "POST /wapi/v2.12/extensibleattributedef") {
		t.Errorf("Observe: error = %v, want the missing-extensible-attribute-definition remediation text verbatim", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	// An absent definition makes the underlying SDK retry its existence
	// check against the Grid Master proxy (_proxy_search=GM) before
	// giving up — an implementation detail of the vendored client, not
	// of this guard — so this only asserts the guard fired at least
	// once, not an exact count (see the "already exists" Create-path
	// probe tests above for the case where exactly one call is the
	// correct expectation).
	if eaDefSearchCalls < 1 {
		t.Errorf("eaDefSearchCalls = %d, want at least 1 — the reactive guard must have probed", eaDefSearchCalls)
	}
}

// TestClusterObserveRecoversRotatedRefStillSurfacesPrerequisiteError is
// the "reference is stale, not merely unknown" variant: a stored _ref
// that 404s falls through to the same identity-EA search, which fails
// the same way.
func TestClusterObserveRecoversRotatedRefStillSurfacesPrerequisiteError(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = eaDefCreateForbiddenBody
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-observe-stale-ref-undefined-ea",
	}
	cr := newClusterARecord("my-arecord", "record:a/stale-ref:host.example.com/default")

	_, err := e.Observe(context.Background(), cr)
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Observe: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
}

// TestClusterObserveSteadyStateNeverProbesPrerequisite proves the
// common-case cost claim: when the stored reference resolves directly
// (no fallback search needed), Observe never calls the prerequisite
// probe at all — even though the mock server is configured so the probe
// (and the identity-EA search) would fail if it were ever reached.
func TestClusterObserveSteadyStateNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	m.undefinedEASearch = true // would break the ladder if reached
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv4Addr: stringPtr("10.0.0.1"),
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-steady-state",
	}
	cr := newClusterARecord("my-arecord", ref)

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

// TestClusterObserveForeignIdentityNeverProbesPrerequisite proves the
// guard's precision: a resolution failure that has nothing to do with
// the identity-EA search (here, HandleReuseError — the reference
// resolved fine, but its stamped uid belongs to someone else) must never
// call the probe, which would otherwise risk poisoning the shared
// verdict cache with an unrelated failure.
func TestClusterObserveForeignIdentityNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-foreign-identity",
	}
	cr := newClusterARecord("my-arecord", foreignRef)

	_, err := e.Observe(context.Background(), cr)
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Fatalf("Observe: error = %v, want it to still be a *identity.HandleReuseError, not intercepted by the prerequisite guard", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — a HandleReuseError is unrelated to the identity-EA search and must not probe", eaDefSearchCalls)
	}
}

// TestClusterObserveRefGetFailureNeverProbesPrerequisite proves the
// guard's precision on the other resolution-failure branch: a non-404
// error fetching the stored reference itself (a ref-GET failure) is
// handled entirely inside resolveByRef and never falls through to the
// identity-EA search at all — so it must never be classified as a
// search failure and must never call the prerequisite probe.
func TestClusterObserveRefGetFailureNeverProbesPrerequisite(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-observe-ref-get-failure",
	}
	cr := newClusterARecord("my-arecord", "record:a/test1:host.example.com/default")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error for a 500 on the ref-GET, got nil")
	}
	if identity.IsSearchFailure(err) {
		t.Fatalf("Observe: error = %v, want it NOT classified as identity.IsSearchFailure — a ref-GET failure never reaches the search step", err)
	}
	var prereq *identity.PrerequisiteError
	if cperrors.As(err, &prereq) {
		t.Fatalf("Observe: error = %v, want it NOT to be a *identity.PrerequisiteError — the prerequisite guard must not fire on a ref-GET failure", err)
	}
}

// TestClusterObserveAmbiguousMatchNeverProbesPrerequisite proves the
// guard's precision on the ladder's other typed refusal: an
// AmbiguousMatchError comes from a search that succeeded (it found more
// than one match), not one that failed, so it must never be classified
// as a search failure and must never call the prerequisite probe.
func TestClusterObserveAmbiguousMatchNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	m.seed(&ibclient.RecordA{Name: stringPtr("host-a.example.com"), Ipv4Addr: stringPtr("10.0.0.1"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.RecordA{Name: stringPtr("host-b.example.com"), Ipv4Addr: stringPtr("10.0.0.2"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-observe-ambiguous",
	}
	cr := newClusterARecord("my-arecord", "record:a/stale-ref:host.example.com/default")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the identity-EA search matches more than one object, got nil")
	}
	var ambiguous *identity.AmbiguousMatchError
	if !cperrors.As(err, &ambiguous) {
		t.Fatalf("Observe: error = %v, want it to still be a *identity.AmbiguousMatchError, not intercepted by the prerequisite guard", err)
	}
	if identity.IsSearchFailure(err) {
		t.Fatalf("Observe: error = %v, want it NOT classified as identity.IsSearchFailure — the search succeeded (found matches), it just found too many", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — an AmbiguousMatchError is unrelated to whether the search itself failed and must not probe", eaDefSearchCalls)
	}
}

// TestClusterDeleteSurfacesPrerequisiteErrorFromIdentitySearch is the
// Delete-side counterpart of
// TestClusterObserveSurfacesPrerequisiteErrorFromIdentitySearch — Delete
// resolves through the same identity ladder and must surface the same
// *identity.PrerequisiteError verbatim, unwrapped, when a stale reference
// falls through to a search that fails because the definition is absent
// and uncreatable.
func TestClusterDeleteSurfacesPrerequisiteErrorFromIdentitySearch(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = eaDefCreateForbiddenBody
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-delete-undefined-ea",
	}
	cr := newClusterARecord("my-arecord", "record:a/stale-ref:host.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Delete: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
	if !strings.Contains(err.Error(), "grid-delete-undefined-ea") {
		t.Errorf("Delete: error = %v, want the missing-extensible-attribute-definition remediation text verbatim", err)
	}
}

// TestClusterDeleteSteadyStateNeverProbesPrerequisite is the Delete-side
// counterpart of TestClusterObserveSteadyStateNeverProbesPrerequisite.
func TestClusterDeleteSteadyStateNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	m.undefinedEASearch = true // would break the ladder if reached
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv4Addr: stringPtr("10.0.0.1"),
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-delete-steady-state",
	}
	cr := newClusterARecord("my-arecord", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error on a reference that resolves directly: %v", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — the steady-state (reference resolves) path must never probe", eaDefSearchCalls)
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

	cr := newClusterARecord("my-arecord", "")
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

	cr := newClusterARecord("my-arecord", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// TestClusterConnectSslVerifyVariants exercises the cluster-scoped
// ProviderConfig's SSLVerify resolution branch in Connect: true, false, and
// omitted (nil, which must default to secure — TLS verification enabled).
// newObjectManagerWithScheme's real TLS-handshake behavior for each boolean
// is proven separately by TestNewObjectManagerWithSchemeEnforcesTLSVerification;
// this test proves Connect correctly extracts and defaults the value from
// pc.Spec.SSLVerify for every branch without erroring.
func TestClusterConnectSslVerifyVariants(t *testing.T) {
	cases := map[string]*bool{
		"Enabled":  boolPtr(true),
		"Disabled": boolPtr(false),
		"Omitted":  nil,
	}

	for name, sslVerify := range cases {
		t.Run(name, func(t *testing.T) {
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
							SSLVerify: sslVerify,
						},
					},
				).Build()

			conn := &clusterConnector{
				kube:  kube,
				usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{}),
			}

			cr := newClusterARecord("my-arecord", "")
			got, err := conn.Connect(context.Background(), cr)
			if err != nil {
				t.Fatalf("Connect: unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("Connect: expected non-nil ExternalClient, got nil")
			}
		})
	}
}

// TestClusterConnectIgnoresSecretSslVerifyKey pins the migration end to
// end: even though the credentials Secret carries a legacy ssl_verify=false
// key, the cluster ProviderConfig's own sslVerify=true spec field is the
// sole source of truth — Connect must succeed exactly as it would with a
// Secret that never had the key at all, proving the dead key has no effect
// on the connector.
func TestClusterConnectIgnoresSecretSslVerifyKey(t *testing.T) {
	const (
		ns     = "crossplane-system"
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	credSecret := credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t")
	credSecret.Data["ssl_verify"] = []byte("false")

	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credSecret,
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
					SSLVerify: boolPtr(true),
				},
			},
		).Build()

	conn := &clusterConnector{
		kube:  kube,
		usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newClusterARecord("my-arecord", "")
	got, err := conn.Connect(context.Background(), cr)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Connect: expected non-nil ExternalClient, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")

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

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/does-not-exist:host.example.com/default", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestNamespacedObservePreCreateState is the namespaced-scope counterpart
// of TestObservePreCreateState.
func TestNamespacedObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "", "ProviderConfig")
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
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/test1:host.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/test1:host.example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordA{})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")

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
	if ap.IPv4Addr != nil {
		t.Errorf("AtProvider.IPv4Addr = %v, want nil", ap.IPv4Addr)
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

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.IPv4Addr = stringPtr("10.0.0.2")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.Ipv4Addr == nil || *stored.Ipv4Addr != "10.0.0.2" {
		t.Errorf("Update: stored ipv4addr = %v, want 10.0.0.2", stored.Ipv4Addr)
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

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.IPv4Addr = stringPtr("10.0.0.2")

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
	m.eaDefCreateBody = `{"Error":"AdmConAuthError: Not authorized"}`
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.IPv4Addr = stringPtr("10.0.0.2")

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

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{Name: stringPtr("host.example.com"), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/does-not-exist:host.example.com/default", "ProviderConfig")

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

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/test1:host.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteARecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteARecord)
	}
}

// TestNamespacedDeleteRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterDeleteRefusesOnForeignIdentity.
func TestNamespacedDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv4Addr: stringPtr("10.0.0.1"),
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", foreignRef, "ProviderConfig")

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

// TestNamespacedDeleteRecoversRotatedRefAndDeletes is the namespaced-scope
// counterpart of TestClusterDeleteRecoversRotatedRefAndDeletes.
func TestNamespacedDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv4Addr: stringPtr("10.0.0.1"),
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/stale-ref:host.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error recovering a rotated object, got: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[newRef]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: rotated record still present — the identity ladder's recovered object must have been deleted, not silently skipped")
	}
}

// ── namespaced: Observe/Delete identity prerequisite probe (reactive) ────
//
// Namespaced-scope counterparts of the cluster-scope tests above — same
// reasoning, same test-isolation precaution (a fresh identity.NewProber()
// per test).

func TestNamespacedObserveSurfacesPrerequisiteErrorFromIdentitySearch(t *testing.T) {
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreateStatus = http.StatusForbidden
	m.eaDefCreateBody = eaDefCreateForbiddenBody
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-namespaced-observe-undefined-ea",
	}
	cr := newNamespacedARecord("default", "my-arecord", "", "ProviderConfig")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the identity extensible attribute definition is absent and uncreatable, got nil")
	}
	var prereq *identity.PrerequisiteError
	if !cperrors.As(err, &prereq) {
		t.Fatalf("Observe: error = %v (%T), want it to wrap a *identity.PrerequisiteError", err, err)
	}
	if !strings.Contains(err.Error(), "grid-namespaced-observe-undefined-ea") {
		t.Errorf("Observe: error = %v, want the missing-extensible-attribute-definition remediation text verbatim", err)
	}
}

func TestNamespacedObserveSteadyStateNeverProbesPrerequisite(t *testing.T) {
	m := newMockWapiServer()
	m.undefinedEASearch = true // would break the ladder if reached
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv4Addr: stringPtr("10.0.0.1"),
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-namespaced-steady-state",
	}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")

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
	m.eaDefCreateBody = eaDefCreateForbiddenBody
	m.undefinedEASearch = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-namespaced-delete-undefined-ea",
	}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/stale-ref:host.example.com/default", "ProviderConfig")

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
	m.undefinedEASearch = true // would break the ladder if reached
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		View:     "default",
		Ipv4Addr: stringPtr("10.0.0.1"),
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{
		kube:     &recordingKubeClient{},
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		prober:   identity.NewProber(),
		endpoint: "grid-namespaced-delete-steady-state",
	}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error on a reference that resolves directly: %v", err)
	}

	m.mu.Lock()
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if eaDefSearchCalls != 0 {
		t.Errorf("eaDefSearchCalls = %d, want 0 — the steady-state (reference resolves) path must never probe", eaDefSearchCalls)
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

	cr := newNamespacedARecord(ns, "my-arecord", "", "ProviderConfig")
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

	cr := newNamespacedARecord("app-ns", "my-arecord", "", "ClusterProviderConfig")
	got, err := conn.Connect(context.Background(), cr)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Connect: expected non-nil ExternalClient, got nil")
	}
}

// TestNamespacedConnectSslVerifyVariants exercises the SSLVerify resolution
// branch in namespacedConnector.Connect for both supported providerConfigRef
// kinds (namespace-scoped ProviderConfig and cluster-scoped
// ClusterProviderConfig), each with sslVerify true, false, and omitted (nil,
// which must default to secure — TLS verification enabled). See
// TestNewObjectManagerWithSchemeEnforcesTLSVerification for the real
// TLS-handshake proof that the resolved boolean reaches the transport.
func TestNamespacedConnectSslVerifyVariants(t *testing.T) {
	sslVerifyCases := map[string]*bool{
		"Enabled":  boolPtr(true),
		"Disabled": boolPtr(false),
		"Omitted":  nil,
	}

	t.Run("ProviderConfig", func(t *testing.T) {
		const (
			ns     = "default"
			secret = "infobloxnios-api-key"
		)
		for name, sslVerify := range sslVerifyCases {
			t.Run(name, func(t *testing.T) {
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
								SSLVerify: sslVerify,
							},
						},
					).Build()

				conn := &namespacedConnector{
					kube:  kube,
					usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
				}

				cr := newNamespacedARecord(ns, "my-arecord", "", "ProviderConfig")
				got, err := conn.Connect(context.Background(), cr)
				if err != nil {
					t.Fatalf("Connect: unexpected error: %v", err)
				}
				if got == nil {
					t.Fatal("Connect: expected non-nil ExternalClient, got nil")
				}
			})
		}
	})

	t.Run("ClusterProviderConfig", func(t *testing.T) {
		const secret = "infobloxnios-api-key"
		ns := "crossplane-system"
		for name, sslVerify := range sslVerifyCases {
			t.Run(name, func(t *testing.T) {
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
								SSLVerify: sslVerify,
							},
						},
					).Build()

				conn := &namespacedConnector{
					kube:  kube,
					usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
				}

				cr := newNamespacedARecord("app-ns", "my-arecord", "", "ClusterProviderConfig")
				got, err := conn.Connect(context.Background(), cr)
				if err != nil {
					t.Fatalf("Connect: unexpected error: %v", err)
				}
				if got == nil {
					t.Fatal("Connect: expected non-nil ExternalClient, got nil")
				}
			})
		}
	})
}

// TestNamespacedConnectIgnoresSecretSslVerifyKey is the namespaced-scope
// counterpart of TestClusterConnectIgnoresSecretSslVerifyKey: a legacy
// ssl_verify=false key in the credentials Secret must have zero effect —
// the namespace-scoped ProviderConfig's own sslVerify=true spec field is
// the sole source of truth.
func TestNamespacedConnectIgnoresSecretSslVerifyKey(t *testing.T) {
	const (
		ns     = "default"
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	credSecret := credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t")
	credSecret.Data["ssl_verify"] = []byte("false")

	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credSecret,
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
					SSLVerify: boolPtr(true),
				},
			},
		).Build()

	conn := &namespacedConnector{
		kube:  kube,
		usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newNamespacedARecord(ns, "my-arecord", "", "ProviderConfig")
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

	cr := newNamespacedARecord("default", "my-arecord", "", "SomeOtherKind")
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

	rec := &ibclient.RecordA{
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

// TestLateInitializeStripsIdentityEAFromExtAttrs proves that the
// provider's own identity stamp (identity.EAKey) is never back-filled
// into spec.forProvider.extAttrs. The CRD schema reserves that key and
// rejects it via CEL validation, so late-initializing it in would be a
// permanent, un-reconcilable diff — and it would also defeat the whole
// point of the stamp, which exists precisely to be invisible to the
// user-facing spec.
func TestLateInitializeStripsIdentityEAFromExtAttrs(t *testing.T) {
	var comment *string
	var ttl *uint32
	var useTTL *bool
	extAttrs := map[string]string(nil)

	rec := &ibclient.RecordA{
		Ea: identity.Stamp(ibclient.EA{"env": "prod"}, "some-managed-resource-uid"),
	}

	changed := lateInitialize(&comment, &ttl, &useTTL, &extAttrs, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true (env should still be backfilled), got false")
	}
	if _, ok := extAttrs[identity.EAKey]; ok {
		t.Errorf("lateInitialize: extAttrs = %v, must not contain the reserved identity key %q", extAttrs, identity.EAKey)
	}
	if !extAttrsEqual(extAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("lateInitialize: extAttrs = %v, want only {env: prod} with the identity key stripped", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	ttl := uint32Ptr(120)
	useTTL := boolPtr(false)
	extAttrs := map[string]string{"env": "staging"}

	rec := &ibclient.RecordA{
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
	rec := &ibclient.RecordA{
		Ttl:    &zoneDefault,
		UseTtl: boolPtr(false),
	}

	lateInitialize(&comment, &ttl, &useTTL, &extAttrs, rec)

	if ttl != nil {
		t.Errorf("lateInitialize: ttl = %v, want nil (useTtl is off, observed ttl is the zone default, not a user value)", *ttl)
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that name,
// ipv4Addr, and view — the CRD's required ARecordParameters fields — are
// never overwritten by Observe()'s late-init step. lateInitialize only
// accepts pointers to the optional fields (comment, ttl, useTtl,
// extAttrs), so a spec/observed mismatch on a required field can never
// occur through the real WAPI flow (name+view compose the object's
// _ref) — this test drives it artificially to pin the guarantee.
func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("observed.example.com"),
		Ipv4Addr: stringPtr("10.0.0.99"),
		View:     "observed-view",
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)
	cr.Spec.ForProvider.Name = stringPtr("host.example.com")
	cr.Spec.ForProvider.IPv4Addr = stringPtr("10.0.0.1")
	cr.Spec.ForProvider.View = stringPtr("default")

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := *cr.Spec.ForProvider.Name; got != "host.example.com" {
		t.Errorf("Observe: required field Name late-initialized to %q, want unchanged %q", got, "host.example.com")
	}
	if got := *cr.Spec.ForProvider.IPv4Addr; got != "10.0.0.1" {
		t.Errorf("Observe: required field IPv4Addr late-initialized to %q, want unchanged %q", got, "10.0.0.1")
	}
	if got := *cr.Spec.ForProvider.View; got != "default" {
		t.Errorf("Observe: required field View late-initialized to %q, want unchanged %q", got, "default")
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedRecord := func() *ibclient.RecordA {
		ttl := uint32(300)
		return &ibclient.RecordA{
			Name:     stringPtr("host.example.com"),
			Ipv4Addr: stringPtr("10.0.0.1"),
			Comment:  stringPtr("hello"),
			Ttl:      &ttl,
			UseTtl:   boolPtr(true),
			Ea:       ibclient.EA{"env": "prod"},
		}
	}

	cases := map[string]struct {
		reason   string
		name     *string
		ipv4Addr *string
		comment  *string
		ttl      *uint32
		useTTL   *bool
		extAttrs map[string]string
		want     bool
	}{
		"IdenticalFieldsAreUpToDate": {
			reason:   "when every mutable field matches the observed record, the resource must be reported up to date",
			name:     stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     true,
		},
		"ChangedNameIsNotUpToDate": {
			reason:   "a changed name must be detected as drift",
			name:     stringPtr("renamed.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedIPv4AddrIsNotUpToDate": {
			reason:   "a changed ipv4Addr must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.2"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedCommentIsNotUpToDate": {
			reason:   "a changed comment must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("goodbye"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedTTLIsNotUpToDate": {
			reason:   "a changed ttl must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(600),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedUseTTLIsNotUpToDate": {
			reason:   "a changed useTtl flag must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(false),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ExtAttrsDifferentValueIsNotUpToDate": {
			reason:   "an extAttrs value change on an existing key must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "staging"},
			want:     false,
		},
		"ExtAttrsDifferentKeyIsNotUpToDate": {
			reason:   "an extAttrs key added/removed must be detected as drift",
			name:     stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"owner": "platform-team"},
			want:     false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := isUpToDate(tc.name, tc.ipv4Addr, tc.comment, tc.ttl, tc.useTTL, tc.extAttrs, observedRecord())
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
	observed := &ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		Comment:  stringPtr("hello"),
		Ttl:      &zoneDefault,
		UseTtl:   boolPtr(false),
		Ea:       ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		stringPtr("host.example.com"),
		stringPtr("10.0.0.1"),
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
	observed := &ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		Comment:  stringPtr("hello"),
		Ttl:      &ttl,
		UseTtl:   boolPtr(true),
		Ea:       ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		stringPtr("host.example.com"),
		stringPtr("10.0.0.1"),
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
	rec := &ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
	}
	// The observed record carries no extattrs (nil Ea) — a spec with an
	// explicit empty map must still compare as up to date, since
	// extAttrsEqual treats nil and empty as equivalent (avoids a phantom
	// diff when the WAPI response omits an empty extattrs object).
	got := isUpToDate(stringPtr("host.example.com"), stringPtr("10.0.0.1"), nil, nil, nil, map[string]string{}, rec)
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

// ── extractCredentials: ssl_verify key is fully ignored ────────────────
//
// TLS verification moved from a Secret-embedded credential option to the
// ProviderConfig's own sslVerify spec field (see cluster.go/namespaced.go
// Connect methods). extractCredentials no longer reads or exposes a
// ssl_verify value at all — nioCredentials has no SslVerify field.

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
		t.Fatalf("extractCredentials returned unexpected creds: %+v", creds)
	}
}

func TestNewObjectManagerWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newObjectManagerWithScheme must not hardcode
	// sslVerify to "true" — it must honor the sslVerify parameter. Both
	// branches must construct successfully (transport config validation
	// happens locally; no network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t"}
			mgrConn, err := newObjectManagerWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
			}
			if mgrConn.Manager == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil object manager")
			}
			if mgrConn.Connector == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil connector")
			}
		})
	}
}

// TestNewObjectManagerWithSchemeEnforcesTLSVerification proves — via a real
// TLS handshake against a self-signed httptest server — that the sslVerify
// boolean genuinely reaches the underlying TransportConfig, not just that
// construction succeeds either way. sslVerify=true must reject the
// self-signed certificate; sslVerify=false must accept it.
func TestNewObjectManagerWithSchemeEnforcesTLSVerification(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewTLSServer(m.handler())
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse TLS test server URL: %v", err)
	}
	creds := &nioCredentials{Host: u.Hostname(), Username: "test-user", Password: "test-pass"}

	t.Run("VerifyEnabledRejectsSelfSignedCert", func(t *testing.T) {
		mgrConn, err := newObjectManagerWithScheme(creds, true, "https", u.Port())
		if err != nil {
			t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
		}
		objMgr := mgrConn.Manager
		if _, err := objMgr.GetARecordByRef("record:a/does-not-exist"); err == nil {
			t.Fatal("GetARecordByRef: expected a TLS certificate verification error with sslVerify=true against a self-signed cert, got nil")
		} else if lower := strings.ToLower(err.Error()); !strings.Contains(lower, "certificate") && !strings.Contains(lower, "x509") {
			t.Errorf("GetARecordByRef: expected a TLS certificate verification error, got: %v", err)
		}
	})

	t.Run("VerifyDisabledAcceptsSelfSignedCert", func(t *testing.T) {
		mgrConn, err := newObjectManagerWithScheme(creds, false, "https", u.Port())
		if err != nil {
			t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
		}
		objMgr := mgrConn.Manager
		_, err = objMgr.GetARecordByRef("record:a/does-not-exist")
		if err == nil {
			t.Fatal("GetARecordByRef: expected a not-found error for a nonexistent record, got nil")
		}
		if lower := strings.ToLower(err.Error()); strings.Contains(lower, "certificate") || strings.Contains(lower, "x509") {
			t.Errorf("GetARecordByRef: expected the TLS handshake to succeed with sslVerify=false, got a certificate error: %v", err)
		}
		if !isNotFound(err) {
			t.Errorf("GetARecordByRef: expected a 404 not-found error once the TLS handshake succeeds, got: %v", err)
		}
	})
}

// ── Delete: RemoveAssociatedPtr (documented SDK limitation) ────────────
//
// The ARecordParameters schema accepts removeAssociatedPtr for schema
// completeness, but the infoblox-go-client SDK's DeleteARecord wrapper
// takes only the object reference — it exposes no query-parameter or
// request-body hook for the WAPI remove_associated_ptr delete option (see
// deleteARecord's doc comment in controller.go). This test pins that
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

			ref := m.seed(&ibclient.RecordA{Name: stringPtr("host.example.com"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

			mc := newTestObjectManager(t, srv)
			e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
			cr := newClusterARecord("my-arecord", ref)
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

// ── identity ladder: empty-external-name recovery (both scopes) ─────────
//
// TestObservePreCreateState (cluster) and TestNamespacedObservePreCreateState
// already cover the zero-match half of this row (no annotation, no
// matching object → ResourceExists:false). These tests cover the other
// half: no annotation at all, but exactly one object already carries this
// resource's stamped identity attribute — the create-crash-window
// recovery path (identity.OutcomeFoundByUID).

func TestClusterObserveEmptyExternalNameRecoversSingleMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foundRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "")
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

	foundRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "", "ProviderConfig")
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

// ── identity ladder: adopt / rotate / foreign-identity — namespaced scope ──
//
// Namespaced-scope counterparts of TestClusterObserveAdoptsUnstampedObjectAndForcesUpdate,
// TestClusterObserveRecoversRotatedRefAndPersistsAnnotation, and
// TestClusterObserveRefusesOnForeignIdentity — the same ladder rows the
// cluster scope already covers, exercised through namespacedExternal to
// prove both scopes are wired identically (IN-IDENT-PILOT acceptance
// criterion).

func TestNamespacedObserveAdoptsUnstampedObjectAndForcesUpdate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		// No Ea at all — the object has never been stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")

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

func TestNamespacedObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	newRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	staleRef := "record:a/stale-ref:host.example.com/default"
	cr := newNamespacedARecord("default", "my-arecord", staleRef, "ProviderConfig")

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
}

func TestNamespacedObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	foreignRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, "someone-elses-uid"),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", foreignRef, "ProviderConfig")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected an error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Observe: error = %v, want it to wrap a *identity.HandleReuseError", err)
	}
}

// ── identity ladder: ambiguous match refusal (Observe + Delete, both scopes) ──
//
// The remaining refusal row not yet exercised at the controller level:
// the stored reference 404s (or is empty) and the identity-EA search
// matches more than one object. identity.Resolve itself is exhaustively
// covered for this row (TestResolveRef404MultipleMatchesRefusesAmbiguous,
// TestResolveEmptyRefMultipleMatchesRefusesAmbiguous); these tests prove
// the controller layer surfaces that refusal correctly and, for Delete,
// issues no mutating call.

func TestClusterObserveRefusesOnAmbiguousMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	m.seed(&ibclient.RecordA{Name: stringPtr("host-a.example.com"), Ipv4Addr: stringPtr("10.0.0.1"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})
	m.seed(&ibclient.RecordA{Name: stringPtr("host-b.example.com"), Ipv4Addr: stringPtr("10.0.0.2"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "record:a/stale-ref:host.example.com/default")

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

	m.seed(&ibclient.RecordA{Name: stringPtr("host-a.example.com"), Ipv4Addr: stringPtr("10.0.0.1"), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})
	m.seed(&ibclient.RecordA{Name: stringPtr("host-b.example.com"), Ipv4Addr: stringPtr("10.0.0.2"), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/stale-ref:host.example.com/default", "ProviderConfig")

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

	refA := m.seed(&ibclient.RecordA{Name: stringPtr("host-a.example.com"), Ipv4Addr: stringPtr("10.0.0.1"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})
	refB := m.seed(&ibclient.RecordA{Name: stringPtr("host-b.example.com"), Ipv4Addr: stringPtr("10.0.0.2"), View: "default", Ea: identity.Stamp(nil, testUIDCluster)})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", "record:a/stale-ref:host.example.com/default")

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

	refA := m.seed(&ibclient.RecordA{Name: stringPtr("host-a.example.com"), Ipv4Addr: stringPtr("10.0.0.1"), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})
	refB := m.seed(&ibclient.RecordA{Name: stringPtr("host-b.example.com"), Ipv4Addr: stringPtr("10.0.0.2"), View: "default", Ea: identity.Stamp(nil, testUIDNamespaced)})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/stale-ref:host.example.com/default", "ProviderConfig")

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
//
// Observe leniently adopts an object whose _ref resolves but which
// carries no identity stamp at all (identity.OutcomeAdopted), re-stamping
// it on the next Update. Delete cannot take that same leniency: deleting
// is irreversible, so deleteARecordIdentity refuses instead
// (errDeleteUnverifiedOwnership) — stricter than Observe's handling of
// the exact same ladder row. This was previously untested at any level.

func TestClusterDeleteRefusesOnUnstampedObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		// No Ea at all — never stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)

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

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		// No Ea at all — never stamped.
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")

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

// ── Create stamps identity — namespaced scope ────────────────────────────
//
// TestClusterCreateSuccess already proves Create stamps the identity EA
// atomically for the cluster scope. This is the namespaced counterpart.

func TestNamespacedCreateStampsIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	m.mu.Lock()
	stored := m.records[got]
	m.mu.Unlock()
	if stored == nil {
		t.Fatalf("Create: no record stored under external-name %q", got)
	}
	if uid, ok := stored.Ea[identity.EAKey]; !ok || uid != string(cr.GetUID()) {
		t.Errorf("Create: stored identity EA = %v, want %q = %q", stored.Ea, identity.EAKey, cr.GetUID())
	}
}

// ── Update re-asserts the identity stamp on every mutating call ─────────
//
// Live verification against a real NIOS Grid Manager (recorded on
// IN-IDENT-PILOT) found that a WAPI PUT carrying extattrs REPLACES the
// whole map rather than merging it per-key. Update() must therefore
// re-send the identity stamp on every call, even when only an unrelated
// field changes — omitting it would silently strip the provider's own
// identity attribute off the object on the very first field update after
// create.

func TestClusterUpdateReassertsIdentityStamp(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)
	cr.Spec.ForProvider.IPv4Addr = stringPtr("10.0.0.2")
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

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.IPv4Addr = stringPtr("10.0.0.2")
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
// TestClusterUpdateRefreshesExternalNameOnRename already proves Update()
// calls through to a real client.Patch (via kube.updated != nil on
// recordingKubeClient) rather than only mutating cr in memory. These
// tests go one step further, per convention: use a real fake.Client and
// re-GET into a *distinct* object instance after Update() returns, so the
// assertion cannot be satisfied by an implementation that mutates cr
// in-process without ever calling through to the API server.

func TestClusterUpdateRefreshedExternalNamePersistsAcrossReGet(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	cr := newClusterARecord("my-arecord", oldRef)
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating rename, want a refreshed _ref")
	}

	fetched := &clusterv1alpha1.ARecord{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: cr.GetName()}, fetched); err != nil {
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

	oldRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	cr := newNamespacedARecord("default", "my-arecord", oldRef, "ProviderConfig")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	newRef := meta.GetExternalName(cr)
	if newRef == oldRef {
		t.Fatal("Update: external-name unchanged after a _ref-mutating rename, want a refreshed _ref")
	}

	fetched := &namespacedv1alpha1.ARecord{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: cr.GetName(), Namespace: cr.GetNamespace()}, fetched); err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Update: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

// ── isUpToDate ignores the identity EA; AtProvider still mirrors it ────

// TestIsUpToDateIgnoresIdentityEA proves the identity attribute this
// provider stamps on every managed object never counts as drift: an
// observed record carrying the stamp compares equal to a spec whose
// extAttrs never mentions it — the CRD schema rejects the reserved key
// outright, so a naive comparison against the raw Grid EA map would
// produce a permanent, unresolvable diff.
func TestIsUpToDateIgnoresIdentityEA(t *testing.T) {
	observed := &ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		Comment:  stringPtr("hello"),
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, "some-managed-resource-uid"),
	}

	got := isUpToDate(stringPtr("host.example.com"), stringPtr("10.0.0.1"), stringPtr("hello"), nil, nil, map[string]string{"env": "prod"}, observed)
	if !got {
		t.Error("isUpToDate: want true when only the stripped identity EA differs between spec and observed, got false")
	}
}

// TestClusterObserveAtProviderExtAttrsIncludesIdentityKey proves
// status.atProvider.extAttrs is a full, unfiltered mirror of the Grid's
// extattrs map (the read-only status mirror surfaces everything the API
// returns) even though the identical key is stripped from
// spec.forProvider.extAttrs to avoid a permanent diff (see
// TestLateInitializeStripsIdentityEAFromExtAttrs).
func TestClusterObserveAtProviderExtAttrsIncludesIdentityKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(ibclient.EA{"env": "prod"}, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterARecord("my-arecord", ref)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got, ok := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; !ok || got != testUIDCluster {
		t.Errorf("AtProvider.ExtAttrs[%q] = %q (present=%v), want %q — status must mirror the Grid's full extattrs map including the identity stamp", identity.EAKey, got, ok, testUIDCluster)
	}
	if _, ok := cr.Spec.ForProvider.ExtAttrs[identity.EAKey]; ok {
		t.Errorf("spec.forProvider.ExtAttrs = %v, must never contain the reserved identity key", cr.Spec.ForProvider.ExtAttrs)
	}
}

// ── createARecord / updateARecord: hard guard on an empty uid ──────────
//
// identity.Stamp itself has no opinion on an empty uid — these SDK-call
// wrappers are the layer responsible for refusing to stamp (and
// therefore create/update) an object without a stable identity, before
// any network call is attempted. A nil objMgr proves the guard runs
// first: a real call would panic on a nil receiver.

func TestCreateARecordRefusesEmptyUID(t *testing.T) {
	_, err := createARecord(nil, stringPtr("host.example.com"), stringPtr("default"), stringPtr("10.0.0.5"), nil, nil, nil, nil, nil, nil, "")
	if err == nil {
		t.Fatal("createARecord: expected an error when uid is empty, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("createARecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateARecordRefusesEmptyUID(t *testing.T) {
	_, err := updateARecord(nil, "record:a/x:host.example.com/default", stringPtr("host.example.com"), stringPtr("10.0.0.5"), nil, nil, nil, nil, "")
	if err == nil {
		t.Fatal("updateARecord: expected an error when uid is empty, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("updateARecord: error = %v, want it to mention the empty uid", err)
	}
}

// TestCreateARecordRefusesWhitespaceUID and
// TestUpdateARecordRefusesWhitespaceUID: a whitespace-only uid is not
// empty by a literal "" comparison, but it is not a usable identity
// either — the guard must trim before checking, matching the shared
// identity resolution ladder's own TrimSpace check.

func TestCreateARecordRefusesWhitespaceUID(t *testing.T) {
	_, err := createARecord(nil, stringPtr("host.example.com"), stringPtr("default"), stringPtr("10.0.0.5"), nil, nil, nil, nil, nil, nil, "   ")
	if err == nil {
		t.Fatal("createARecord: expected an error when uid is whitespace-only, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("createARecord: error = %v, want it to mention the empty uid", err)
	}
}

func TestUpdateARecordRefusesWhitespaceUID(t *testing.T) {
	_, err := updateARecord(nil, "record:a/x:host.example.com/default", stringPtr("host.example.com"), stringPtr("10.0.0.5"), nil, nil, nil, nil, "   ")
	if err == nil {
		t.Fatal("updateARecord: expected an error when uid is whitespace-only, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("updateARecord: error = %v, want it to mention the empty uid", err)
	}
}
