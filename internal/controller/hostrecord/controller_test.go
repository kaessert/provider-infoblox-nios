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
	}, "http", u.Port())
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
		Ipv6Addrs: []ibclient.HostRecordIpv6Addr{{Ipv6Addr: stringPtr("2001:db8::1")}},
	}

	changed := lateInitialize(&comment, &ttl, &useTTL, &extAttrs, &view, &configureForDNS, &disable, &aliases, &ipv6Addrs, rec)
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
		Ipv6Addrs: []ibclient.HostRecordIpv6Addr{{Ipv6Addr: stringPtr("2001:db8::1")}},
	}

	changed := lateInitialize(&comment, &ttl, &useTTL, &extAttrs, &view, &configureForDNS, &disable, &aliases, &ipv6Addrs, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" || *ttl != 120 || *useTTL != false || extAttrs["env"] != "staging" {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
	if *view != "user-view" || *configureForDNS != false || *disable != false {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
	if len(ipv6Addrs) != 1 || ipv6Addrs[0].Ipv6Addr != "2001:db8::9" {
		t.Error("lateInitialize: overwrote already-set ipv6Addrs")
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that name and
// ipv4Addrs — the CRD's required HostRecordParameters fields — are never
// overwritten by Observe()'s late-init step. lateInitialize only accepts
// pointers to the optional fields, so a spec/observed mismatch on a
// required field can never occur through the real WAPI flow — this test
// drives it artificially to pin the guarantee.
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
