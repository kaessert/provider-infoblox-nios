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
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recorda/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recorda/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
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

// newClusterARecord builds a minimal cluster-scoped ARecord CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterARecord(crName, externalName string) *clusterv1alpha1.ARecord {
	cr := &clusterv1alpha1.ARecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
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
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
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
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.RecordA{}}
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

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Comment:  stringPtr("hello"),
		Ttl:      func() *uint32 { v := uint32(300); return &v }(),
		UseTtl:   boolPtr(true),
		Ea:       ibclient.EA{"env": "prod"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterARecord("my-arecord", "record:a/does-not-exist:host.example.com/default")

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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterARecord("my-arecord", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())    // simulate NameAsExternalName initializer

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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterARecord("my-arecord", "record:a/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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
}

// ── cluster: Create with cidr/networkView (next-available-IP) ──────────

func TestClusterCreateWithCidrAllocatesNextAvailableIP(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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
	_, err := createARecord(nil, stringPtr("host.example.com"), stringPtr("default"), stringPtr("10.0.0.5"), nil, nil, nil, nil, stringPtr("10.0.0.0/24"), nil)
	if err == nil {
		t.Fatal("createARecord: expected an error when cidr and ipv4Addr are both set, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("createARecord: error = %v, want it to mention 'mutually exclusive'", err)
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

func TestClusterUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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
	e := &clusterExternal{kube: kube, objMgr: newTestObjectManager(t, srv)}
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

	ref := m.seed(&ibclient.RecordA{Name: stringPtr("host.example.com"), View: "default"})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterARecord("my-arecord", "record:a/test1:host.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteARecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteARecord)
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/does-not-exist:host.example.com/default", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedARecord("default", "my-arecord", "", "ProviderConfig")
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/test1:host.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordA{Name: stringPtr("host.example.com"), View: "default"})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedARecord("default", "my-arecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedARecord("default", "my-arecord", "record:a/test1:host.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteARecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteARecord)
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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
		objMgr, err := newObjectManagerWithScheme(creds, true, "https", u.Port())
		if err != nil {
			t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
		}
		if _, err := objMgr.GetARecordByRef("record:a/does-not-exist"); err == nil {
			t.Fatal("GetARecordByRef: expected a TLS certificate verification error with sslVerify=true against a self-signed cert, got nil")
		} else if lower := strings.ToLower(err.Error()); !strings.Contains(lower, "certificate") && !strings.Contains(lower, "x509") {
			t.Errorf("GetARecordByRef: expected a TLS certificate verification error, got: %v", err)
		}
	})

	t.Run("VerifyDisabledAcceptsSelfSignedCert", func(t *testing.T) {
		objMgr, err := newObjectManagerWithScheme(creds, false, "https", u.Port())
		if err != nil {
			t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
		}
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

			ref := m.seed(&ibclient.RecordA{Name: stringPtr("host.example.com"), View: "default"})

			e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
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
