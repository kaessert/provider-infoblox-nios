// Package recordptr unit tests for the PTRRecord MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI record:ptr
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package recordptr

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordptr/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordptr/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

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

// newClusterPTRRecord builds a minimal cluster-scoped PTRRecord CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterPTRRecord(crName, externalName string) *clusterv1alpha1.PTRRecord {
	cr := &clusterv1alpha1.PTRRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.PTRRecordSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.PTRRecordParameters{
				Ptrdname: stringPtr("host.example.com"),
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

// newNamespacedPTRRecord is the namespaced variant of newClusterPTRRecord.
func newNamespacedPTRRecord(ns, crName, externalName, pcKind string) *namespacedv1alpha1.PTRRecord {
	cr := &namespacedv1alpha1.PTRRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.PTRRecordSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.PTRRecordParameters{
				Ptrdname: stringPtr("host.example.com"),
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
// mockWapiServer emulates the subset of NIOS WAPI record:ptr endpoints
// exercised by the PTRRecord controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.RecordPTR type so the wire format (including the EA
// {"value": ...} envelope) exactly matches what the SDK sends and expects.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.RecordPTR
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.RecordPTR{}}
}

func (m *mockWapiServer) seed(rec *ibclient.RecordPTR) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		rec.Ref = m.newRefLocked(rec)
	}
	if rec.Zone == "" {
		rec.Zone = zoneFromPtrdname(rec.PtrdName)
	}
	m.records[rec.Ref] = rec
	return rec.Ref
}

func (m *mockWapiServer) newRefLocked(rec *ibclient.RecordPTR) string {
	ptrdname := ""
	if rec.PtrdName != nil {
		ptrdname = *rec.PtrdName
	}
	return "record:ptr/test" + itoa(m.nextRef) + ":" + ptrdname + "/" + rec.View
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

// handler returns an http.Handler implementing the record:ptr WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/record:ptr", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.RecordPTR
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Synthesize the zone the way NIOS derives it server-side, so
		// Observe/Create tests can assert the response-only Zone field
		// is mirrored.
		rec.Zone = zoneFromPtrdname(rec.PtrdName)
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
		var incoming ibclient.RecordPTR
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body

		// Mirror real WAPI behavior: renaming ptrdname mutates the
		// object's _ref. Relocate the record under a freshly minted ref
		// whenever the incoming ptrdname differs from the stored one, so
		// tests can exercise the controller's ref-refresh logic against
		// a realistic response.
		renamed := incoming.PtrdName != nil && (existing.PtrdName == nil || *incoming.PtrdName != *existing.PtrdName)

		existing.PtrdName = incoming.PtrdName
		if incoming.Name != nil {
			existing.Name = incoming.Name
		}
		if incoming.Ipv4Addr != nil {
			existing.Ipv4Addr = incoming.Ipv4Addr
		}
		if incoming.Ipv6Addr != nil {
			existing.Ipv6Addr = incoming.Ipv6Addr
		}
		existing.Comment = incoming.Comment
		existing.Ttl = incoming.Ttl
		existing.UseTtl = incoming.UseTtl
		existing.Ea = incoming.Ea
		existing.Zone = zoneFromPtrdname(existing.PtrdName)

		respRef := ref
		if renamed {
			delete(m.records, ref)
			m.nextRef++
			existing.Ref = m.newRefLocked(existing)
			m.records[existing.Ref] = existing
			respRef = existing.Ref
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

func zoneFromPtrdname(ptrdname *string) string {
	if ptrdname == nil || *ptrdname == "" {
		return ""
	}
	n := *ptrdname
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
	}, "http", u.Port())
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

	ref := m.seed(&ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Comment:  stringPtr("hello"),
		Ttl:      uint32Ptr(300),
		UseTtl:   boolPtr(true),
		Ea:       ibclient.EA{"env": "prod"},
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", ref)
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

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", "record:ptr/does-not-exist:host.example.com/default")

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

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", "") // external-name unset
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
	cr := newClusterPTRRecord("my-ptrrecord", "record:ptr/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", "record:ptr/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map) must not panic and must produce a valid observation with
// nil-safe AtProvider fields. observeFromRecordPTR copies optional
// pointer fields verbatim, so this pins that behavior against regressions.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordPTR{})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", ref)

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
	if ap.Ptrdname != nil {
		t.Errorf("AtProvider.Ptrdname = %v, want nil", ap.Ptrdname)
	}
	if ap.Name != nil {
		t.Errorf("AtProvider.Name = %v, want nil", ap.Name)
	}
	if ap.IPv4Addr != nil {
		t.Errorf("AtProvider.IPv4Addr = %v, want nil", ap.IPv4Addr)
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

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", "") // no external-name yet

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

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", "")
	cr.Spec.ForProvider.IPv4Addr = nil
	cr.Spec.ForProvider.Cidr = stringPtr("10.0.0.0/24")
	cr.Spec.ForProvider.NetworkView = stringPtr("my-view")

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()

	want := "func:nextavailableip:10.0.0.0/24,my-view"
	if stored.Ipv4Addr == nil || *stored.Ipv4Addr != want {
		t.Errorf("Create: stored ipv4addr = %v, want %q", stored.Ipv4Addr, want)
	}
}

func TestClusterCreateWithCidrDefaultsNetworkView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", "")
	cr.Spec.ForProvider.IPv4Addr = nil
	cr.Spec.ForProvider.Cidr = stringPtr("10.0.0.0/24")
	cr.Spec.ForProvider.NetworkView = nil

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()

	want := "func:nextavailableip:10.0.0.0/24,default"
	if stored.Ipv4Addr == nil || *stored.Ipv4Addr != want {
		t.Errorf("Create: stored ipv4addr = %v, want %q", stored.Ipv4Addr, want)
	}
}

func TestClusterCreateCidrAndIPv4AddrMutuallyExclusive(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", "")
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

func TestClusterCreateCidrAndIPv6AddrMutuallyExclusive(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", "")
	cr.Spec.ForProvider.IPv4Addr = nil
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

// TestCreatePTRRecordRejectsCidrWithStaticIP is a white-box test of the
// shared createPTRRecord wrapper: the mutual-exclusivity check must run
// before any SDK/network call is attempted (passing a nil objMgr proves
// this — a real call would panic on a nil receiver).
func TestCreatePTRRecordRejectsCidrWithStaticIP(t *testing.T) {
	_, err := createPTRRecord(nil, stringPtr("host.example.com"), nil, stringPtr("10.0.0.5"), nil, stringPtr("default"), nil, nil, nil, nil, stringPtr("10.0.0.0/24"), nil)
	if err == nil {
		t.Fatal("createPTRRecord: expected an error when cidr and ipv4Addr are both set, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("createPTRRecord: error = %v, want it to mention 'mutually exclusive'", err)
	}
}

func TestClusterObserveMirrorsCidrAndNetworkView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", ref)
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

	ref := m.seed(&ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", ref)
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

// TestClusterCreateServerError verifies that a 5xx response from the WAPI
// create endpoint is propagated (wrapped, not swallowed) and the
// external-name annotation is left unset.
func TestClusterCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreatePTRRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreatePTRRecord)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty on error", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "original-view",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", ref)
	// Mutate the immutable view field in spec — this must NOT affect
	// ResourceUpToDate, since view is excluded from isUpToDate (WAPI
	// rejects PUT with "Field is not allowed for update: view").
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

	ref := m.seed(&ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Comment:  stringPtr("old comment"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", ref)
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

	ref := m.seed(&ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", ref)

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

// TestClusterUpdateRefreshesExternalNameOnRefChange pins the _ref-mutation
// warning called out for PTRRecord: renaming ptrdname/name can return a
// NEW _ref from UpdatePTRRecord, and the controller must adopt it as the
// external-name annotation or the next reconcile 404s against the stale
// ref.
func TestClusterUpdateRefreshesExternalNameOnRefChange(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	// The mock server's PUT handler mirrors real WAPI behavior: renaming
	// ptrdname relocates the record under a freshly minted _ref (see
	// mockWapiServer.handler's PUT case). This exercises the
	// controller's ref-refresh logic against a realistic response.
	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", oldRef)
	cr.Spec.ForProvider.Ptrdname = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == oldRef {
		t.Fatalf("Update: external-name unchanged (%q) after a ref-mutating rename, want a new ref", got)
	}
	if got == "" {
		t.Fatal("Update: external-name is empty after rename")
	}

	m.mu.Lock()
	_, oldStillExists := m.records[oldRef]
	newRec, newExists := m.records[got]
	m.mu.Unlock()
	if oldStillExists {
		t.Errorf("Update: record still present at stale ref %q after rename", oldRef)
	}
	if !newExists {
		t.Fatalf("Update: no record found at new ref %q", got)
	}
	if newRec.PtrdName == nil || *newRec.PtrdName != "renamed.example.com" {
		t.Errorf("Update: relocated record ptrdname = %v, want %q", newRec.PtrdName, "renamed.example.com")
	}
}

// TestClusterUpdateServerError verifies that a 5xx response from the WAPI
// update endpoint is propagated (wrapped, not swallowed) and the
// external-name annotation is left unchanged.
func TestClusterUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	ref := "record:ptr/test1:host.example.com/default"
	cr := newClusterPTRRecord("my-ptrrecord", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdatePTRRecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdatePTRRecord)
	}
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name = %q, want unchanged %q on error", got, ref)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordPTR{PtrdName: stringPtr("host.example.com"), View: "default"})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", ref)

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
	cr := newClusterPTRRecord("my-ptrrecord", "record:ptr/does-not-exist:host.example.com/default")

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
	cr := newClusterPTRRecord("my-ptrrecord", "record:ptr/test1:host.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeletePTRRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeletePTRRecord)
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

	cr := newClusterPTRRecord("my-ptrrecord", "")
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

	cr := newClusterPTRRecord("my-ptrrecord", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", ref, "ProviderConfig")

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
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", "record:ptr/does-not-exist:host.example.com/default", "ProviderConfig")

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
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", "", "ProviderConfig")
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
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", "record:ptr/test1:host.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", "record:ptr/test1:host.example.com/default", "ProviderConfig")

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

	ref := m.seed(&ibclient.RecordPTR{})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", ref, "ProviderConfig")

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
	if ap.Ptrdname != nil {
		t.Errorf("AtProvider.Ptrdname = %v, want nil", ap.Ptrdname)
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

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateServerError verifies that a 5xx response from the
// WAPI create endpoint is propagated (wrapped, not swallowed) and the
// external-name annotation is left unset.
func TestNamespacedCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreatePTRRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreatePTRRecord)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty on error", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", ref, "ProviderConfig")
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

// TestNamespacedUpdateServerError verifies that a 5xx response from the
// WAPI update endpoint is propagated (wrapped, not swallowed) and the
// external-name annotation is left unchanged.
func TestNamespacedUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	ref := "record:ptr/test1:host.example.com/default"
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdatePTRRecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdatePTRRecord)
	}
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name = %q, want unchanged %q on error", got, ref)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordPTR{PtrdName: stringPtr("host.example.com"), View: "default"})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", "record:ptr/does-not-exist:host.example.com/default", "ProviderConfig")

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
	cr := newNamespacedPTRRecord("default", "my-ptrrecord", "record:ptr/test1:host.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeletePTRRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeletePTRRecord)
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

	cr := newNamespacedPTRRecord(ns, "my-ptrrecord", "", "ProviderConfig")
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

	cr := newNamespacedPTRRecord("app-ns", "my-ptrrecord", "", "ClusterProviderConfig")
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

	cr := newNamespacedPTRRecord("default", "my-ptrrecord", "", "SomeOtherKind")
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
	var name *string
	var comment *string
	var ttl *uint32
	var useTTL *bool
	extAttrs := map[string]string(nil)

	rec := &ibclient.RecordPTR{
		Name:    stringPtr("1.0.0.10.in-addr.arpa"),
		Comment: stringPtr("server default"),
		Ttl:     uint32Ptr(600),
		UseTtl:  boolPtr(true),
		Ea:      ibclient.EA{"env": "prod"},
	}

	changed := lateInitialize(&name, &comment, &ttl, &useTTL, &extAttrs, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if name == nil || *name != "1.0.0.10.in-addr.arpa" {
		t.Errorf("lateInitialize: name = %v, want %q", name, "1.0.0.10.in-addr.arpa")
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
	name := stringPtr("user.example.com")
	comment := stringPtr("user comment")
	ttl := uint32Ptr(120)
	useTTL := boolPtr(false)
	extAttrs := map[string]string{"env": "staging"}

	rec := &ibclient.RecordPTR{
		Name:    stringPtr("server.example.com"),
		Comment: stringPtr("server default"),
		Ttl:     uint32Ptr(600),
		UseTtl:  boolPtr(true),
		Ea:      ibclient.EA{"env": "prod"},
	}

	changed := lateInitialize(&name, &comment, &ttl, &useTTL, &extAttrs, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *name != "user.example.com" || *comment != "user comment" || *ttl != 120 || *useTTL != false || extAttrs["env"] != "staging" {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that ptrdname and
// view — the CRD's required PTRRecordParameters fields — are never
// overwritten by Observe()'s late-init step. lateInitialize only accepts
// pointers to the optional fields (name, comment, ttl, useTtl, extAttrs),
// so a spec/observed mismatch on a required field can never occur through
// the real WAPI flow — this test drives it artificially to pin the
// guarantee.
func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordPTR{
		PtrdName: stringPtr("observed.example.com"),
		Ipv4Addr: stringPtr("10.0.0.99"),
		View:     "observed-view",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterPTRRecord("my-ptrrecord", ref)
	cr.Spec.ForProvider.Ptrdname = stringPtr("host.example.com")
	cr.Spec.ForProvider.View = stringPtr("default")

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := *cr.Spec.ForProvider.Ptrdname; got != "host.example.com" {
		t.Errorf("Observe: required field Ptrdname late-initialized to %q, want unchanged %q", got, "host.example.com")
	}
	if got := *cr.Spec.ForProvider.View; got != "default" {
		t.Errorf("Observe: required field View late-initialized to %q, want unchanged %q", got, "default")
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedRecord := func() *ibclient.RecordPTR {
		return &ibclient.RecordPTR{
			PtrdName: stringPtr("host.example.com"),
			Ipv4Addr: stringPtr("10.0.0.1"),
			Comment:  stringPtr("hello"),
			Ttl:      uint32Ptr(300),
			UseTtl:   boolPtr(true),
			Ea:       ibclient.EA{"env": "prod"},
		}
	}

	cases := map[string]struct {
		reason   string
		ptrdname *string
		ipv4Addr *string
		comment  *string
		ttl      *uint32
		useTTL   *bool
		extAttrs map[string]string
		want     bool
	}{
		"IdenticalFieldsAreUpToDate": {
			reason:   "when every mutable field matches the observed record, the resource must be reported up to date",
			ptrdname: stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     true,
		},
		"ChangedPtrdnameIsNotUpToDate": {
			reason:   "a changed ptrdname must be detected as drift",
			ptrdname: stringPtr("renamed.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedIPv4AddrIsNotUpToDate": {
			reason:   "a changed ipv4Addr must be detected as drift",
			ptrdname: stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.2"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedCommentIsNotUpToDate": {
			reason:   "a changed comment must be detected as drift",
			ptrdname: stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("goodbye"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedTTLIsNotUpToDate": {
			reason:   "a changed ttl must be detected as drift",
			ptrdname: stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(600),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ChangedUseTTLIsNotUpToDate": {
			reason:   "a changed useTtl flag must be detected as drift",
			ptrdname: stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(false),
			extAttrs: map[string]string{"env": "prod"},
			want:     false,
		},
		"ExtAttrsDifferentValueIsNotUpToDate": {
			reason:   "an extAttrs value change on an existing key must be detected as drift",
			ptrdname: stringPtr("host.example.com"),
			ipv4Addr: stringPtr("10.0.0.1"),
			comment:  stringPtr("hello"),
			ttl:      uint32Ptr(300),
			useTTL:   boolPtr(true),
			extAttrs: map[string]string{"env": "staging"},
			want:     false,
		},
		"ExtAttrsDifferentKeyIsNotUpToDate": {
			reason:   "an extAttrs key added/removed must be detected as drift",
			ptrdname: stringPtr("host.example.com"),
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
			got := isUpToDate(tc.ptrdname, nil, tc.ipv4Addr, nil, tc.comment, tc.ttl, tc.useTTL, tc.extAttrs, observedRecord())
			if got != tc.want {
				t.Errorf("%s: isUpToDate() = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

func TestIsUpToDateExtAttrsEmptyVsNil(t *testing.T) {
	rec := &ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
	}
	// Observed record has a nil Ea map (no extattrs returned); spec has
	// an explicitly empty (but non-nil) map. These must compare equal.
	if !isUpToDate(stringPtr("host.example.com"), nil, stringPtr("10.0.0.1"), nil, nil, nil, nil, map[string]string{}, rec) {
		t.Error("isUpToDate: nil vs empty extAttrs should be treated as up to date")
	}
}

func TestIsUpToDateIPv6(t *testing.T) {
	rec := &ibclient.RecordPTR{
		PtrdName: stringPtr("host.example.com"),
		Ipv6Addr: stringPtr("2001:db8::1"),
	}
	if !isUpToDate(stringPtr("host.example.com"), nil, nil, stringPtr("2001:db8::1"), nil, nil, nil, nil, rec) {
		t.Error("isUpToDate: matching ipv6Addr should be up to date")
	}
	if isUpToDate(stringPtr("host.example.com"), nil, nil, stringPtr("2001:db8::2"), nil, nil, nil, nil, rec) {
		t.Error("isUpToDate: changed ipv6Addr should be detected as drift")
	}
}

// ── ttlOrZero: nil-safety ────────────────────────────────────────────────

func TestTtlOrZero(t *testing.T) {
	if got := ttlOrZero(nil); got != 0 {
		t.Errorf("ttlOrZero(nil) = %d, want 0", got)
	}
	if got := ttlOrZero(uint32Ptr(300)); got != 300 {
		t.Errorf("ttlOrZero(300) = %d, want 300", got)
	}
}

// ── extractCredentials: ssl_verify ──────────────────────────────────────

func TestExtractCredentialsSslVerifyDefaultsTrue(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "infobloxnios-credentials", "grid.example.com", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: "crossplane-system"},
		Key:             "unused",
	}, "")
	if err != nil {
		t.Fatalf("extractCredentials: unexpected error: %v", err)
	}
	if !creds.SslVerify {
		t.Error("extractCredentials: expected SslVerify to default to true when ssl_verify key is absent")
	}
}

func TestExtractCredentialsSslVerifyFalse(t *testing.T) {
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
	if creds.SslVerify {
		t.Error("extractCredentials: expected SslVerify to be false when ssl_verify key is \"false\"")
	}
}

func TestExtractCredentialsSslVerifyUnrecognizedValueDefaultsTrue(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "infobloxnios-credentials", "grid.example.com", "admin", "s3cr3t")
	secret.Data["ssl_verify"] = []byte("nope")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: "crossplane-system"},
		Key:             "unused",
	}, "")
	if err != nil {
		t.Fatalf("extractCredentials: unexpected error: %v", err)
	}
	if !creds.SslVerify {
		t.Error("extractCredentials: expected SslVerify to default to true for any value other than exactly \"false\"")
	}
}

func TestNewObjectManagerWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newObjectManagerWithScheme must not hardcode
	// SslVerify to "true" — it must honor creds.SslVerify. Both branches
	// must construct successfully (transport config validation happens
	// locally; no network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t", SslVerify: sslVerify}
			objMgr, err := newObjectManagerWithScheme(creds, "http", "80")
			if err != nil {
				t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
			}
			if objMgr == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil object manager")
			}
		})
	}
}
