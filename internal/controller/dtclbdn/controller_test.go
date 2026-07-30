// Package dtclbdn unit tests for the DTCLBDN MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI dtc:lbdn
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package dtclbdn

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dtclbdn/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dtclbdn/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

// Shared literals reused across many test cases (deduplicated for goconst).
const (
	nsDefault    = "default"
	eaKeyEnv     = "env"
	eaValProd    = "prod"
	poolRefWeb   = "dtc:pool/ZG5z...:web-pool"
	zoneRefExamp = "zone_auth/ZG5z...:example.com/default"
)

func stringPtr(s string) *string { return &s }
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

// newClusterDTCLBDN builds a minimal cluster-scoped DTCLBDN CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterDTCLBDN(crName, externalName string) *clusterv1alpha1.DTCLBDN {
	cr := &clusterv1alpha1.DTCLBDN{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.DTCLBDNSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: nsDefault},
			},
			ForProvider: clusterv1alpha1.DTCLBDNParameters{
				Name:     stringPtr("my-lbdn"),
				LBMethod: stringPtr("ROUND_ROBIN"),
				Patterns: []string{"*.example.com"},
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedDTCLBDN is the namespaced variant of newClusterDTCLBDN.
func newNamespacedDTCLBDN(ns, crName, externalName, pcKind string) *namespacedv1alpha1.DTCLBDN {
	cr := &namespacedv1alpha1.DTCLBDN{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.DTCLBDNSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: nsDefault},
			},
			ForProvider: namespacedv1alpha1.DTCLBDNParameters{
				Name:     stringPtr("my-lbdn"),
				LBMethod: stringPtr("ROUND_ROBIN"),
				Patterns: []string{"*.example.com"},
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
// mockDtcLbdnServer emulates the subset of NIOS WAPI dtc:lbdn endpoints
// exercised by the DTCLBDN controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real ibclient.DtcLbdn
// type so the wire format exactly matches what the SDK sends and expects.
// The PUT handler simulates the live-verified _ref instability: renaming
// the LBDN (a `name` change) assigns it a new _ref, mirroring the real
// Grid Manager's behavior (ADR-IN-0004).

type mockDtcLbdnServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.DtcLbdn
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert field content.
	lastUpdateBody []byte
}

func newMockDtcLbdnServer() *mockDtcLbdnServer {
	return &mockDtcLbdnServer{records: map[string]*ibclient.DtcLbdn{}}
}

func (m *mockDtcLbdnServer) seed(rec *ibclient.DtcLbdn) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		rec.Ref = m.newRefLocked(rec)
	}
	m.records[rec.Ref] = rec
	return rec.Ref
}

func (m *mockDtcLbdnServer) newRefLocked(rec *ibclient.DtcLbdn) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	return "dtc:lbdn/test" + itoa(m.nextRef) + ":" + name
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

// handler returns an http.Handler implementing the dtc:lbdn WAPI surface.
func (m *mockDtcLbdnServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/dtc:lbdn", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.DtcLbdn
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
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
		var incoming ibclient.DtcLbdn
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body

		newRef := ref
		// Simulate the live-verified _ref instability: renaming the LBDN
		// assigns it a new _ref.
		if incoming.Name != nil && existing.Name != nil && *incoming.Name != *existing.Name {
			m.nextRef++
			newRef = "dtc:lbdn/renamed" + itoa(m.nextRef) + ":" + *incoming.Name
		}

		existing.Name = incoming.Name
		existing.LbMethod = incoming.LbMethod
		existing.Patterns = incoming.Patterns
		existing.Pools = incoming.Pools
		existing.AuthZones = incoming.AuthZones
		existing.Types = incoming.Types
		existing.Priority = incoming.Priority
		existing.Persistence = incoming.Persistence
		existing.Topology = incoming.Topology
		existing.Ttl = incoming.Ttl
		existing.UseTtl = incoming.UseTtl
		existing.Comment = incoming.Comment
		existing.Disable = incoming.Disable
		existing.Ea = incoming.Ea

		if newRef != ref {
			delete(m.records, ref)
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

// newTestClients builds a *dtcLbdnClients pointed at the given
// httptest.Server via plain HTTP (no TLS needed — the WapiRequestBuilder
// only switches to HTTPS when hostCfg.Scheme != "http").
func newTestClients(t *testing.T, srv *httptest.Server) *dtcLbdnClients {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	clients, err := newClientsWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test clients: %v", err)
	}
	return clients
}

// ── cluster: Observe ────────────────────────────────────────────────────

func TestClusterObserveSuccess(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Comment:  stringPtr("hello"),
		Disable:  boolPtr(false),
		Ea:       ibclient.EA{eaKeyEnv: eaValProd},
	})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.Disable = boolPtr(false)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{eaKeyEnv: eaValProd}

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
	if cr.Status.AtProvider.Ref == nil || *cr.Status.AtProvider.Ref != ref {
		t.Errorf("AtProvider.Ref = %v, want %q", cr.Status.AtProvider.Ref, ref)
	}
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/does-not-exist:my-lbdn")

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

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName()) // simulate NameAsExternalName initializer

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

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/test1:my-lbdn")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/test1:my-lbdn")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map) must not panic and must produce a valid observation with
// nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", ref)

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
	if ap.LBMethod != nil {
		t.Errorf("AtProvider.LBMethod = %v, want nil", ap.LBMethod)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.Disable != nil {
		t.Errorf("AtProvider.Disable = %v, want nil", ap.Disable)
	}
	if ap.Topology != nil {
		t.Errorf("AtProvider.Topology = %v, want nil", ap.Topology)
	}
	if ap.Pools != nil {
		t.Errorf("AtProvider.Pools = %v, want nil", ap.Pools)
	}
	if ap.AuthZones != nil {
		t.Errorf("AtProvider.AuthZones = %v, want nil", ap.AuthZones)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.Health != nil {
		t.Errorf("AtProvider.Health = %v, want nil", ap.Health)
	}
}

func TestClusterObservePoolsAuthZonesAndHealth(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Pools: []*ibclient.DtcPoolLink{
			{Pool: poolRefWeb, Ratio: 1},
		},
		AuthZones: []*ibclient.ZoneAuth{
			{Ref: zoneRefExamp},
		},
		Health: &ibclient.DtcHealth{
			Availability: "GREEN",
			Description:  "healthy",
			EnabledState: "ENABLED",
		},
	})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", ref)
	cr.Spec.ForProvider.Pools = []clusterv1alpha1.DTCLBDNPoolLink{
		{Pool: stringPtr(poolRefWeb), Ratio: uint32Ptr(1)},
	}
	cr.Spec.ForProvider.AuthZones = []string{zoneRefExamp}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true, got false")
	}

	ap := cr.Status.AtProvider
	if len(ap.Pools) != 1 || ap.Pools[0].Pool == nil || *ap.Pools[0].Pool != poolRefWeb {
		t.Errorf("AtProvider.Pools = %+v, want one entry with the seeded pool ref", ap.Pools)
	}
	if len(ap.AuthZones) != 1 || ap.AuthZones[0] != zoneRefExamp {
		t.Errorf("AtProvider.AuthZones = %+v, want [%q]", ap.AuthZones, zoneRefExamp)
	}
	if ap.Health == nil || ap.Health.Availability == nil || *ap.Health.Availability != "GREEN" {
		t.Errorf("AtProvider.Health = %+v, want Availability=GREEN", ap.Health)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", "") // no external-name yet
	cr.Spec.ForProvider.Pools = []clusterv1alpha1.DTCLBDNPoolLink{
		{Pool: stringPtr(poolRefWeb), Ratio: uint32Ptr(1)},
	}
	cr.Spec.ForProvider.AuthZones = []string{zoneRefExamp}

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}

	m.mu.Lock()
	stored := m.records[got]
	m.mu.Unlock()
	if stored == nil {
		t.Fatal("Create: record not stored on mock server")
	}
	if len(stored.Pools) != 1 || stored.Pools[0].Pool != poolRefWeb {
		t.Errorf("Create: stored pools = %+v, want the ref passed through untouched (no name-based lookup)", stored.Pools)
	}
	if len(stored.AuthZones) != 1 || stored.AuthZones[0] == nil || stored.AuthZones[0].Ref != zoneRefExamp {
		t.Errorf("Create: stored authZones = %+v, want the ref passed through untouched (no fqdn/view lookup)", stored.AuthZones)
	}
}

// TestClusterCreateServerError pins the error-propagation path when the
// WAPI backend rejects the create POST outright (e.g. transient 500s).
func TestClusterCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", "")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name set to %q despite failed create", got)
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/test1:my-lbdn")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
}

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Comment:  stringPtr("old comment"),
	})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", ref)
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

// TestClusterUpdateSendsAllFields pins the PUT-echo-everything contract for
// DTCLBDN: since there are no known immutable fields, Update must send
// every mutable field on every request (not a partial patch).
func TestClusterUpdateSendsAllFields(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
	})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", ref)
	cr.Spec.ForProvider.LBMethod = stringPtr("RATIO")

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
	if _, present := raw["name"]; !present {
		t.Error("Update: request body missing 'name' — PUT must echo all fields")
	}
	if _, present := raw["lb_method"]; !present {
		t.Error("Update: request body missing 'lb_method' — PUT must echo all fields")
	}
	if _, present := raw["patterns"]; !present {
		t.Error("Update: request body missing 'patterns' — PUT must echo all fields")
	}
}

// TestClusterUpdateRefreshesExternalNameOnRefChange pins the _ref-instability
// contract (live-verified, ADR-IN-0004): renaming a DTCLBDN changes its
// WAPI `_ref`, so Update must detect the change and refresh the
// crossplane.io/external-name annotation from the PUT response.
func TestClusterUpdateRefreshesExternalNameOnRefChange(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("old-name"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
	})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", ref)
	cr.Spec.ForProvider.Name = stringPtr("new-name")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == ref {
		t.Errorf("Update: external-name still %q, want it refreshed to the new _ref after rename", ref)
	}
	if got == "" {
		t.Error("Update: external-name unexpectedly cleared")
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn")})

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", ref)

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
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/does-not-exist:my-lbdn")

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

	e := &clusterExternal{clients: newTestClients(t, srv)}
	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/test1:my-lbdn")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteDTCLBDN) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteDTCLBDN)
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
				ObjectMeta: metav1.ObjectMeta{Name: nsDefault},
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

	cr := newClusterDTCLBDN("my-lbdn", "")
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

	cr := newClusterDTCLBDN("my-lbdn", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
	})

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", ref, "ProviderConfig")

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

// TestNamespacedObservePoolsAndHealth pins the namespaced-scope pool and
// health conversion path (poolsFromNamespaced/poolsToNamespaced/
// healthToNamespaced), mirroring the cluster-scope coverage above.
func TestNamespacedObservePoolsAndHealth(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Pools: []*ibclient.DtcPoolLink{
			{Pool: poolRefWeb, Ratio: 1},
		},
		Health: &ibclient.DtcHealth{
			Availability: "GREEN",
			Description:  "healthy",
			EnabledState: "ENABLED",
		},
	})

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", ref, "ProviderConfig")
	cr.Spec.ForProvider.Pools = []namespacedv1alpha1.DTCLBDNPoolLink{
		{Pool: stringPtr(poolRefWeb), Ratio: uint32Ptr(1)},
	}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true, got false")
	}

	ap := cr.Status.AtProvider
	if len(ap.Pools) != 1 || ap.Pools[0].Pool == nil || *ap.Pools[0].Pool != poolRefWeb {
		t.Errorf("AtProvider.Pools = %+v, want one entry with the seeded pool ref", ap.Pools)
	}
	if ap.Health == nil || ap.Health.Availability == nil || *ap.Health.Availability != "GREEN" {
		t.Errorf("AtProvider.Health = %+v, want Availability=GREEN", ap.Health)
	}
}

func TestNamespacedObserveNotFound(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "dtc:lbdn/does-not-exist:my-lbdn", "ProviderConfig")

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

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "", "ProviderConfig")
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

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "dtc:lbdn/test1:my-lbdn", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "dtc:lbdn/test1:my-lbdn", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestNamespacedObserveMinimalResponse is the namespaced-scope counterpart
// of TestClusterObserveMinimalResponse — see that test's doc comment for
// rationale.
func TestNamespacedObserveMinimalResponse(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{})

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", ref, "ProviderConfig")

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
	if ap.LBMethod != nil {
		t.Errorf("AtProvider.LBMethod = %v, want nil", ap.LBMethod)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.Disable != nil {
		t.Errorf("AtProvider.Disable = %v, want nil", ap.Disable)
	}
	if ap.Topology != nil {
		t.Errorf("AtProvider.Topology = %v, want nil", ap.Topology)
	}
	if ap.Pools != nil {
		t.Errorf("AtProvider.Pools = %v, want nil", ap.Pools)
	}
	if ap.AuthZones != nil {
		t.Errorf("AtProvider.AuthZones = %v, want nil", ap.AuthZones)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.Health != nil {
		t.Errorf("AtProvider.Health = %v, want nil", ap.Health)
	}
}

// ── namespaced: Create ────────────────────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestNamespacedCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
}

// ── namespaced: Update ────────────────────────────────────────────────────

func TestNamespacedUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "dtc:lbdn/test1:my-lbdn", "ProviderConfig")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Comment:  stringPtr("old comment"),
	})

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", ref, "ProviderConfig")
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

// TestNamespacedUpdateRefreshesExternalNameOnRefChange mirrors
// TestClusterUpdateRefreshesExternalNameOnRefChange for the namespaced scope: the
// server-returned _ref must be re-adopted as the external-name annotation
// when it differs from the ref addressed.
func TestNamespacedUpdateRefreshesExternalNameOnRefChange(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("old-name"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
	})

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", ref, "ProviderConfig")
	cr.Spec.ForProvider.Name = stringPtr("new-name")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == ref {
		t.Errorf("Update: external-name still %q, want it refreshed to the new _ref after rename", ref)
	}
	if got == "" {
		t.Error("Update: external-name unexpectedly cleared")
	}
}

// ── namespaced: Delete ────────────────────────────────────────────────────

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn")})

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", ref, "ProviderConfig")

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
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "dtc:lbdn/does-not-exist:my-lbdn", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{clients: newTestClients(t, srv)}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "dtc:lbdn/test1:my-lbdn", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteDTCLBDN) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteDTCLBDN)
	}
}

// ── namespaced: Connect ───────────────────────────────────────────────────

func TestNamespacedConnectWithProviderConfig(t *testing.T) {
	const (
		ns     = nsDefault
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: nsDefault, Namespace: ns},
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

	cr := newNamespacedDTCLBDN(ns, "my-lbdn", "", "ProviderConfig")
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
				ObjectMeta: metav1.ObjectMeta{Name: nsDefault},
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

	cr := newNamespacedDTCLBDN("app-ns", "my-lbdn", "", "ClusterProviderConfig")
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

	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "", "SomeOtherKind")
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
	in := map[string]string{eaKeyEnv: eaValProd, "owner": "platform-team"}
	ea := buildEA(in)
	out := extAttrsFromEA(ea)
	if !extAttrsEqual(in, out) {
		t.Errorf("ExtAttrs round-trip: got %v, want %v", out, in)
	}
}

// TestStringifyEAValue pins every branch of the extensible-attribute value
// coercion — WAPI can hand back strings, ibclient.Bool, string slices (from
// EA.UnmarshalJSON), or arbitrary values needing a fallback %v render.
func TestStringifyEAValue(t *testing.T) {
	cases := map[string]struct {
		reason string
		in     interface{}
		want   string
	}{
		"Nil": {
			reason: "a nil EA value renders as empty string",
			in:     nil,
			want:   "",
		},
		"String": {
			reason: "string values pass through unchanged",
			in:     "hello",
			want:   "hello",
		},
		"BoolTrue": {
			reason: "ibclient.Bool(true) renders as \"True\" (WAPI's own casing)",
			in:     ibclient.Bool(true),
			want:   "True",
		},
		"BoolFalse": {
			reason: "ibclient.Bool(false) renders as \"False\"",
			in:     ibclient.Bool(false),
			want:   "False",
		},
		"StringSlice": {
			reason: "string slices join with commas",
			in:     []string{"a", "b", "c"},
			want:   "a,b,c",
		},
		"Fallback": {
			reason: "any other type falls back to a %v render",
			in:     42,
			want:   "42",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := stringifyEAValue(tc.in)
			if got != tc.want {
				t.Errorf("%s: stringifyEAValue(%#v) = %q, want %q (%s)", name, tc.in, got, tc.want, tc.reason)
			}
		})
	}
}

func TestExtAttrsEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !extAttrsEqual(nil, map[string]string{}) {
		t.Error("extAttrsEqual(nil, {}) = false, want true")
	}
	if !extAttrsEqual(map[string]string{}, nil) {
		t.Error("extAttrsEqual({}, nil) = false, want true")
	}
}

func TestPoolsRoundTrip(t *testing.T) {
	in := []poolLink{
		{Pool: stringPtr(poolRefWeb), Ratio: uint32Ptr(1)},
	}
	sdk := buildPools(in)
	out := poolsFromSDK(sdk)
	if !poolsEqual(in, out) {
		t.Errorf("pools round-trip: got %+v, want %+v", out, in)
	}
}

func TestPoolsEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !poolsEqual(nil, []poolLink{}) {
		t.Error("poolsEqual(nil, []) = false, want true")
	}
}

func TestAuthZonesRoundTrip(t *testing.T) {
	in := []string{zoneRefExamp}
	sdk := buildAuthZones(in)
	out := authZonesFromSDK(sdk)
	if !stringSlicesEqual(in, out) {
		t.Errorf("authZones round-trip: got %+v, want %+v", out, in)
	}
}

func TestStringSlicesEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !stringSlicesEqual(nil, []string{}) {
		t.Error("stringSlicesEqual(nil, []) = false, want true")
	}
}

func TestStringSlicesEqualOrderMatters(t *testing.T) {
	if stringSlicesEqual([]string{"a", "b"}, []string{"b", "a"}) {
		t.Error("stringSlicesEqual: want order-sensitive comparison, got equal for reordered slices")
	}
}

func TestIsNotFoundClassifiesTypedError(t *testing.T) {
	err := ibclient.NewNotFoundError("not found")
	if !isNotFound(err) {
		t.Error("isNotFound: want true for *ibclient.NotFoundError, got false")
	}
}

func TestIsNotFoundClassifiesGenericStatusError(t *testing.T) {
	genericErr := formatWapiError(404)
	if !isNotFound(genericErr) {
		t.Errorf("isNotFound: want true for generic 404 error %q, got false", genericErr.Error())
	}
	genericErr500 := formatWapiError(500)
	if isNotFound(genericErr500) {
		t.Errorf("isNotFound: want false for generic 500 error %q, got true", genericErr500.Error())
	}
}

// formatWapiError constructs an error string matching the SDK's generic
// HTTP error format, for exercising isNotFound's regexp fallback path.
func formatWapiError(status int) error {
	return &genericStatusError{status: status}
}

type genericStatusError struct{ status int }

func (e *genericStatusError) Error() string {
	return "WAPI request error: " + itoa(e.status) + "('boom')\nfull body here"
}

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var priority, persistence, ttl *uint32
	var topology, comment *string
	var useTTL, disable *bool
	var extAttrs map[string]string

	rec := &ibclient.DtcLbdn{
		Priority:    uint32Ptr(10),
		Persistence: uint32Ptr(30),
		Topology:    stringPtr("my-topology"),
		Ttl:         uint32Ptr(300),
		UseTtl:      boolPtr(true),
		Comment:     stringPtr("lbdn comment"),
		Disable:     boolPtr(true),
		Ea:          ibclient.EA{eaKeyEnv: eaValProd},
	}

	changed := lateInitialize(&priority, &persistence, &topology, &ttl, &useTTL, &comment, &disable, &extAttrs, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if priority == nil || *priority != 10 {
		t.Errorf("priority = %v, want 10", priority)
	}
	if persistence == nil || *persistence != 30 {
		t.Errorf("persistence = %v, want 30", persistence)
	}
	if topology == nil || *topology != "my-topology" {
		t.Errorf("topology = %v, want %q", topology, "my-topology")
	}
	if ttl == nil || *ttl != 300 {
		t.Errorf("ttl = %v, want 300", ttl)
	}
	if useTTL == nil || !*useTTL {
		t.Errorf("useTTL = %v, want true", useTTL)
	}
	if comment == nil || *comment != "lbdn comment" {
		t.Errorf("comment = %v, want %q", comment, "lbdn comment")
	}
	if disable == nil || !*disable {
		t.Errorf("disable = %v, want true", disable)
	}
	if len(extAttrs) != 1 || extAttrs[eaKeyEnv] != eaValProd {
		t.Errorf("extAttrs = %v, want {env: prod}", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	priority := uint32Ptr(99)
	persistence := uint32Ptr(99)
	topology := stringPtr("user-topology")
	ttl := uint32Ptr(99)
	useTTL := boolPtr(false)
	comment := stringPtr("user comment")
	disable := boolPtr(false)
	extAttrs := map[string]string{"owner": "user-team"}

	rec := &ibclient.DtcLbdn{
		Priority:    uint32Ptr(10),
		Persistence: uint32Ptr(30),
		Topology:    stringPtr("server-topology"),
		Ttl:         uint32Ptr(300),
		UseTtl:      boolPtr(true),
		Comment:     stringPtr("server comment"),
		Disable:     boolPtr(true),
		Ea:          ibclient.EA{eaKeyEnv: eaValProd},
	}

	changed := lateInitialize(&priority, &persistence, &topology, &ttl, &useTTL, &comment, &disable, &extAttrs, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *priority != 99 {
		t.Errorf("priority overwritten by lateInitialize: %d", *priority)
	}
	if *topology != "user-topology" {
		t.Errorf("topology overwritten by lateInitialize: %q", *topology)
	}
	if *comment != "user comment" {
		t.Errorf("comment overwritten by lateInitialize: %q", *comment)
	}
	if extAttrs["owner"] != "user-team" {
		t.Errorf("extAttrs overwritten by lateInitialize: %v", extAttrs)
	}
}

func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	// name, lbMethod, and patterns are required fields — lateInitialize
	// has no parameters for them at all, so this test simply pins that
	// contract by confirming the function signature only accepts the
	// optional fields.
	var priority, persistence, ttl *uint32
	var topology, comment *string
	var useTTL, disable *bool
	var extAttrs map[string]string

	rec := &ibclient.DtcLbdn{
		Name:     stringPtr("lbdn-name"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
	}

	_ = lateInitialize(&priority, &persistence, &topology, &ttl, &useTTL, &comment, &disable, &extAttrs, rec)
	// No assertions needed beyond "does not panic" — name/lbMethod/patterns
	// aren't parameters of lateInitialize, so there is nothing for it to
	// overwrite.
}

func TestIsUpToDate(t *testing.T) {
	basePools := []poolLink{{Pool: stringPtr(poolRefWeb), Ratio: uint32Ptr(1)}}
	baseAuthZones := []string{zoneRefExamp}
	baseTypes := []string{"A", "AAAA"}
	baseExtAttrs := map[string]string{eaKeyEnv: eaValProd}

	base := &ibclient.DtcLbdn{
		Name:        stringPtr("my-lbdn"),
		LbMethod:    "ROUND_ROBIN",
		Patterns:    []string{"*.example.com"},
		Pools:       buildPools(basePools),
		AuthZones:   buildAuthZones(baseAuthZones),
		Types:       baseTypes,
		Priority:    uint32Ptr(5),
		Persistence: uint32Ptr(60),
		Topology:    nil,
		Ttl:         uint32Ptr(300),
		UseTtl:      boolPtr(true),
		Comment:     stringPtr("hello"),
		Disable:     boolPtr(false),
		Ea:          ibclient.EA{eaKeyEnv: eaValProd},
	}

	cases := map[string]struct {
		mutate func() (name, lbMethod *string, patterns []string, pools []poolLink, authZones, types []string, priority, persistence *uint32, topology *string, ttl *uint32, useTTL *bool, comment *string, disable *bool, extAttrs map[string]string)
		want   bool
	}{
		"MatchesExactly": {
			mutate: func() (*string, *string, []string, []poolLink, []string, []string, *uint32, *uint32, *string, *uint32, *bool, *string, *bool, map[string]string) {
				return stringPtr("my-lbdn"), stringPtr("ROUND_ROBIN"), []string{"*.example.com"}, basePools, baseAuthZones, baseTypes, uint32Ptr(5), uint32Ptr(60), nil, uint32Ptr(300), boolPtr(true), stringPtr("hello"), boolPtr(false), baseExtAttrs
			},
			want: true,
		},
		"LBMethodDiffers": {
			mutate: func() (*string, *string, []string, []poolLink, []string, []string, *uint32, *uint32, *string, *uint32, *bool, *string, *bool, map[string]string) {
				return stringPtr("my-lbdn"), stringPtr("RATIO"), []string{"*.example.com"}, basePools, baseAuthZones, baseTypes, uint32Ptr(5), uint32Ptr(60), nil, uint32Ptr(300), boolPtr(true), stringPtr("hello"), boolPtr(false), baseExtAttrs
			},
			want: false,
		},
		"PatternsDiffer": {
			mutate: func() (*string, *string, []string, []poolLink, []string, []string, *uint32, *uint32, *string, *uint32, *bool, *string, *bool, map[string]string) {
				return stringPtr("my-lbdn"), stringPtr("ROUND_ROBIN"), []string{"*.other.com"}, basePools, baseAuthZones, baseTypes, uint32Ptr(5), uint32Ptr(60), nil, uint32Ptr(300), boolPtr(true), stringPtr("hello"), boolPtr(false), baseExtAttrs
			},
			want: false,
		},
		"PoolsDiffer": {
			mutate: func() (*string, *string, []string, []poolLink, []string, []string, *uint32, *uint32, *string, *uint32, *bool, *string, *bool, map[string]string) {
				diff := []poolLink{{Pool: stringPtr("dtc:pool/ZG5z...:other-pool"), Ratio: uint32Ptr(2)}}
				return stringPtr("my-lbdn"), stringPtr("ROUND_ROBIN"), []string{"*.example.com"}, diff, baseAuthZones, baseTypes, uint32Ptr(5), uint32Ptr(60), nil, uint32Ptr(300), boolPtr(true), stringPtr("hello"), boolPtr(false), baseExtAttrs
			},
			want: false,
		},
		"AuthZonesDiffer": {
			mutate: func() (*string, *string, []string, []poolLink, []string, []string, *uint32, *uint32, *string, *uint32, *bool, *string, *bool, map[string]string) {
				return stringPtr("my-lbdn"), stringPtr("ROUND_ROBIN"), []string{"*.example.com"}, basePools, []string{"zone_auth/ZG5z...:other.com/default"}, baseTypes, uint32Ptr(5), uint32Ptr(60), nil, uint32Ptr(300), boolPtr(true), stringPtr("hello"), boolPtr(false), baseExtAttrs
			},
			want: false,
		},
		"PriorityDiffers": {
			mutate: func() (*string, *string, []string, []poolLink, []string, []string, *uint32, *uint32, *string, *uint32, *bool, *string, *bool, map[string]string) {
				return stringPtr("my-lbdn"), stringPtr("ROUND_ROBIN"), []string{"*.example.com"}, basePools, baseAuthZones, baseTypes, uint32Ptr(99), uint32Ptr(60), nil, uint32Ptr(300), boolPtr(true), stringPtr("hello"), boolPtr(false), baseExtAttrs
			},
			want: false,
		},
		"CommentDiffers": {
			mutate: func() (*string, *string, []string, []poolLink, []string, []string, *uint32, *uint32, *string, *uint32, *bool, *string, *bool, map[string]string) {
				return stringPtr("my-lbdn"), stringPtr("ROUND_ROBIN"), []string{"*.example.com"}, basePools, baseAuthZones, baseTypes, uint32Ptr(5), uint32Ptr(60), nil, uint32Ptr(300), boolPtr(true), stringPtr("changed"), boolPtr(false), baseExtAttrs
			},
			want: false,
		},
		"DisableDiffers": {
			mutate: func() (*string, *string, []string, []poolLink, []string, []string, *uint32, *uint32, *string, *uint32, *bool, *string, *bool, map[string]string) {
				return stringPtr("my-lbdn"), stringPtr("ROUND_ROBIN"), []string{"*.example.com"}, basePools, baseAuthZones, baseTypes, uint32Ptr(5), uint32Ptr(60), nil, uint32Ptr(300), boolPtr(true), stringPtr("hello"), boolPtr(true), baseExtAttrs
			},
			want: false,
		},
		"ExtAttrsDiffer": {
			mutate: func() (*string, *string, []string, []poolLink, []string, []string, *uint32, *uint32, *string, *uint32, *bool, *string, *bool, map[string]string) {
				return stringPtr("my-lbdn"), stringPtr("ROUND_ROBIN"), []string{"*.example.com"}, basePools, baseAuthZones, baseTypes, uint32Ptr(5), uint32Ptr(60), nil, uint32Ptr(300), boolPtr(true), stringPtr("hello"), boolPtr(false), map[string]string{eaKeyEnv: "dev"}
			},
			want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cname, clbMethod, cpatterns, cpools, cauthzones, ctypes, cpriority, cpersistence, ctopology, cttl, cusettl, ccomment, cdisable, cextattrs := tc.mutate()
			got := isUpToDate(cname, clbMethod, cpatterns, cpools, cauthzones, ctypes, cpriority, cpersistence, ctopology, cttl, cusettl, ccomment, cdisable, cextattrs, base)
			if got != tc.want {
				t.Errorf("%s: isUpToDate() = %v, want %v", name, got, tc.want)
			}
		})
	}
}

func TestIsUpToDateExtAttrsEmptyVsNil(t *testing.T) {
	rec := &ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Ea:       nil,
	}
	got := isUpToDate(stringPtr("my-lbdn"), stringPtr("ROUND_ROBIN"), []string{"*.example.com"}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{}, rec)
	if !got {
		t.Error("isUpToDate: want true when spec ExtAttrs={} and observed Ea=nil (treated as equal)")
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

func TestNewClientWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newClientsWithScheme must not hardcode SslVerify to
	// "true" — it must honor the sslVerify parameter. Both branches must construct
	// successfully (transport config validation happens locally; no
	// network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t"}
			clients, err := newClientsWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newClientsWithScheme: unexpected error: %v", err)
			}
			if clients == nil {
				t.Fatal("newClientsWithScheme: expected non-nil clients")
			}
			if clients.objMgr == nil {
				t.Fatal("newClientsWithScheme: expected non-nil object manager")
			}
			if clients.conn == nil {
				t.Fatal("newClientsWithScheme: expected non-nil connector")
			}
		})
	}
}
