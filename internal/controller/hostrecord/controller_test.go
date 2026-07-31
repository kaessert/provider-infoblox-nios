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
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/hostrecord/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/hostrecord/v1alpha1"
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

// newClusterHostRecord builds a minimal cluster-scoped HostRecord CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterHostRecord(crName, externalName string) *clusterv1alpha1.HostRecord {
	cr := &clusterv1alpha1.HostRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
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
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
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

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte

	// renameRefOnUpdate, when non-empty, simulates NIOS returning a
	// different _ref from the one addressed by PUT (e.g. a DNS-view or
	// name change on a live Grid Manager) — used to exercise the
	// controller's _ref instability handling on Update.
	renameRefOnUpdate string
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.HostRecord{}}
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
		var incoming ibclient.HostRecord
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
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
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"Error":"boom"}`))
	})
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
		Ea:          ibclient.EA{"env": "prod"},
		Disable:     boolPtr(false),
	})

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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
	// Zero-route server: any request is an error, proving Observe never
	// calls it during the pre-create guard.
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())          // simulate NameAsExternalName initializer

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

	e := &clusterExternal{client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/test1:host.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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
// getHostRecordByRef field-set extension (see its doc comment).
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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
	})

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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
// update endpoint is propagated (wrapped, not swallowed).
func TestClusterUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	ref := m.seed(&ibclient.HostRecord{Name: stringPtr("host.example.com"), View: stringPtr("default")})

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
	cr := newClusterHostRecord("my-hostrecord", "record:host/test1:host.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteHostRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteHostRecord)
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

	e := &namespacedExternal{client: newTestClient(t, srv)}
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

	e := &namespacedExternal{client: newTestClient(t, srv)}
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
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "", "ProviderConfig")
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

	e := &namespacedExternal{client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/test1:host.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{client: newTestClient(t, srv)}
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

	e := &namespacedExternal{client: newTestClient(t, srv)}
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

	e := &namespacedExternal{client: newTestClient(t, srv)}
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

	e := &namespacedExternal{client: newTestClient(t, srv)}
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

	e := &namespacedExternal{client: newTestClient(t, srv)}
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

// TestNamespacedUpdateServerError verifies that a 5xx response from the
// WAPI update endpoint is propagated (wrapped, not swallowed).
func TestNamespacedUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{client: newTestClient(t, srv)}
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

	e := &namespacedExternal{client: newTestClient(t, srv)}
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

	ref := m.seed(&ibclient.HostRecord{Name: stringPtr("host.example.com"), View: stringPtr("default")})

	e := &namespacedExternal{client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{client: newTestClient(t, srv)}
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

	e := &namespacedExternal{client: newTestClient(t, srv)}
	cr := newNamespacedHostRecord("default", "my-hostrecord", "record:host/test1:host.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteHostRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteHostRecord)
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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
	})

	e := &clusterExternal{client: newTestClient(t, srv)}
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
	})

	e := &clusterExternal{client: newTestClient(t, srv)}
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
	})

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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

	e := &clusterExternal{client: newTestClient(t, srv)}
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
