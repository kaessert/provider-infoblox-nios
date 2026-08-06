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

	cperrors "github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dtclbdn/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dtclbdn/v1alpha1"
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
			ClusterManagedResourceSpec: xpv2.ClusterManagedResourceSpec{
				ProviderConfigReference: &xpv2.Reference{Name: nsDefault},
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
				ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: pcKind, Name: nsDefault},
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
// Grid Manager's behavior (live-verified).

type mockDtcLbdnServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.DtcLbdn
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert field content.
	lastUpdateBody []byte

	// eaDefExists controls the identity extensible-attribute-definition
	// prerequisite endpoint. Defaults to true via newMockDtcLbdnServer.
	eaDefExists bool
	// eaDefCreatable controls whether a POST to create the missing
	// definition succeeds, when eaDefExists is false.
	eaDefCreatable bool
	// searchCalls counts identity-EA search requests.
	searchCalls int
	// eaDefSearchCalls counts prerequisite-probe GET requests.
	eaDefSearchCalls int
	// createCalls counts POST (create) requests — tests assert this to
	// prove a refusal or a validation failure issued zero mutating
	// requests, not just that the in-memory record set looks unchanged.
	createCalls int
	// deleteCalls counts DELETE requests, for the same reason.
	deleteCalls int
}

func newMockDtcLbdnServer() *mockDtcLbdnServer {
	return &mockDtcLbdnServer{records: map[string]*ibclient.DtcLbdn{}, eaDefExists: true}
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
		m.mu.Lock()
		m.createCalls++
		m.mu.Unlock()
		ref := m.seed(&rec)
		writeJSON(w, http.StatusOK, ref)
	})

	// Search endpoint: a GET with no _ref path segment. The identity
	// ladder (identity.Resolve's searchByUID) filters by the stamped
	// "*Crossplane Internal ID" extensible attribute; legacy tests may
	// still filter by the `name` query param. Registered as an exact
	// literal path so Go's ServeMux prefers it over the {ref...}
	// wildcard below for requests to precisely "dtc:lbdn" (real _refs
	// always carry additional path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/dtc:lbdn", func(w http.ResponseWriter, r *http.Request) {
		uid := r.URL.Query().Get("*" + identity.EAKey)
		name := r.URL.Query().Get("name")

		m.mu.Lock()
		m.searchCalls++
		var matches []ibclient.DtcLbdn
		for _, rec := range m.records {
			if uid != "" {
				got, ok := rec.Ea[identity.EAKey]
				if !ok || got != uid {
					continue
				}
				matches = append(matches, *rec)
				continue
			}
			if name != "" && (rec.Name == nil || *rec.Name != name) {
				continue
			}
			matches = append(matches, *rec)
		}
		m.mu.Unlock()

		// Always respond 200 — WAPI search semantics report "not found"
		// via an empty array, never an HTTP error status.
		writeJSON(w, http.StatusOK, matches)
	})

	// Identity extensible-attribute-definition prerequisite endpoint
	// (see internal/clients/identity's Prober).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.eaDefSearchCalls++
		exists := m.eaDefExists
		m.mu.Unlock()
		if exists {
			writeJSON(w, http.StatusOK, []ibclient.EADefinition{{Name: stringPtr(identity.EAKey)}})
			return
		}
		writeJSON(w, http.StatusOK, []ibclient.EADefinition{})
	})
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		creatable := m.eaDefCreatable
		m.mu.Unlock()
		if creatable {
			m.mu.Lock()
			m.eaDefExists = true
			m.mu.Unlock()
			writeJSON(w, http.StatusOK, "extensibleattributedef/identity-def:"+identity.EAKey)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"Error":"IBDataConflictError: Cannot create extensible attribute definition. Only superusers can manage extensible attribute definition"}`))
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
		m.deleteCalls++
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
		Ea:       ibclient.EA{eaKeyEnv: eaValProd, identity.EAKey: "test-uid-cluster"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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
	if cond := cr.GetCondition(xpv2.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/does-not-exist:my-lbdn")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestObservePreCreateState verifies that Observe runs one identity
// search (not a hard-coded no-op) when the external-name still equals
// the CR's Kubernetes name — the pre-create state for a server-assigned
// external-name strategy. The pre-create guard does not short-circuit:
// it maps the annotation to "" and lets the identity ladder search by
// uid before concluding ResourceExists:false.
func TestObservePreCreateState(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName()) // simulate NameAsExternalName initializer

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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/test1:my-lbdn")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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
		Ea: ibclient.EA{identity.EAKey: "test-uid-cluster"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

// TestClusterUpdatePrerequisiteAutoCreates verifies the
// unconditional Update guard: when the identity extensible attribute
// definition is absent but the configured credential can create one, the
// probe auto-creates it before the mutating PUT, and the update proceeds
// normally — this is the exact path a pre-existing, unstamped object hits
// on every reconcile (Observe resolves it as OutcomeAdopted, forcing
// Update), so the auto-create must be reachable from here, not just from
// Create.
func TestClusterUpdatePrerequisiteAutoCreates(t *testing.T) {
	m := newMockDtcLbdnServer()
	m.eaDefExists = false
	m.eaDefCreatable = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Comment:  stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber()}
	cr := newClusterDTCLBDN("my-lbdn", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	defExists := m.eaDefExists
	m.mu.Unlock()
	if !defExists {
		t.Error("Update: eaDefExists = false, want true — the prerequisite probe must auto-create the identity definition before the mutating call")
	}
}

// TestClusterUpdatePrerequisiteRefusesUncreatable verifies the
// unconditional Update guard on the refusal side: when the identity
// extensible attribute definition is absent and the configured credential
// cannot create one, Update returns the typed PrerequisiteError (not a raw
// wrapped WAPI 400) and issues no mutating call — the object is left
// exactly as it was.
func TestClusterUpdatePrerequisiteRefusesUncreatable(t *testing.T) {
	m := newMockDtcLbdnServer()
	m.eaDefExists = false
	m.eaDefCreatable = false
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Comment:  stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber()}
	cr := newClusterDTCLBDN("my-lbdn", ref)
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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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
// contract (live-verified against a real Grid): renaming a DTCLBDN changes its
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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	ref := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn"), Ea: ibclient.EA{identity.EAKey: "test-uid-cluster"}})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/test1:my-lbdn")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteDTCLBDN) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteDTCLBDN)
	}
}

// TestClusterDeleteRefusesOnForeignIdentity verifies the identity ladder's
// handle-reuse refusal: the stored _ref still resolves, but its stamped
// identity attribute belongs to a different managed resource. Deleting it
// would destroy someone else's object, so Delete() must refuse and leave
// the record in place.
func TestClusterDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", ref)

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Delete: error = %v, want a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestClusterDeleteRefusesOnUnstampedObject verifies the identity ladder's
// adopt-vs-delete asymmetry: the stored _ref resolves but the object
// carries no identity stamp at all. Observe() adopts and re-stamps such
// objects leniently, but Delete() must refuse — destroying an object is
// irreversible and ownership cannot be proven.
func TestClusterDeleteRefusesOnUnstampedObject(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn")})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", ref)

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when the resolved object carries no identity stamp, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestClusterDeleteRecoversRotatedRefAndDeletes verifies rotation
// recovery: the stored _ref 404s, but exactly one live object carries
// this managed resource's identity stamp. Delete() must recover it via
// the identity-EA search and delete the recovered object, not report a
// false already-gone success.
func TestClusterDeleteRecoversRotatedRefAndDeletes(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/stale-ref:my-lbdn")
	liveRef := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn"), Ea: ibclient.EA{identity.EAKey: string(cr.GetUID())}})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error recovering a rotated reference: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: recovered record was not removed")
	}
}

// TestClusterDeleteSucceedsWhenTrulyAbsent is the companion happy path: a
// 404 against the stored _ref, and an identity-EA search that finds
// nothing, means the object really is gone.
func TestClusterDeleteSucceedsWhenTrulyAbsent(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/stale-ref:my-lbdn")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
	}
}

// TestClusterObserveRefusesOnForeignIdentity verifies the Observe()-side
// half of handle-reuse refusal: crossplane-runtime's managed reconciler
// calls Observe() before Delete() on the deletion path, and if Observe()
// silently adopted a foreign object it would let the next Update/Delete
// mutate or destroy someone else's record.
func TestClusterObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", ref)

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Observe: error = %v, want a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live record was removed — Observe() must never mutate the backend")
	}
}

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
				ObjectMeta: metav1.ObjectMeta{Name: nsDefault},
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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
		Ea: ibclient.EA{identity.EAKey: "test-uid-namespaced"},
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "", "ProviderConfig")
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "dtc:lbdn/test1:my-lbdn", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
}

// ── namespaced: Update ────────────────────────────────────────────────────

func TestNamespacedUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

// TestNamespacedUpdatePrerequisiteAutoCreates verifies the
// unconditional Update guard: when the identity extensible attribute
// definition is absent but the configured credential can create one, the
// probe auto-creates it before the mutating PUT, and the update proceeds
// normally — this is the exact path a pre-existing, unstamped object hits
// on every reconcile (Observe resolves it as OutcomeAdopted, forcing
// Update), so the auto-create must be reachable from here, not just from
// Create.
func TestNamespacedUpdatePrerequisiteAutoCreates(t *testing.T) {
	m := newMockDtcLbdnServer()
	m.eaDefExists = false
	m.eaDefCreatable = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Comment:  stringPtr("old comment"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber()}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	defExists := m.eaDefExists
	m.mu.Unlock()
	if !defExists {
		t.Error("Update: eaDefExists = false, want true — the prerequisite probe must auto-create the identity definition before the mutating call")
	}
}

// TestNamespacedUpdatePrerequisiteRefusesUncreatable verifies the
// unconditional Update guard on the refusal side: when the identity
// extensible attribute definition is absent and the configured credential
// cannot create one, Update returns the typed PrerequisiteError (not a raw
// wrapped WAPI 400) and issues no mutating call — the object is left
// exactly as it was.
func TestNamespacedUpdatePrerequisiteRefusesUncreatable(t *testing.T) {
	m := newMockDtcLbdnServer()
	m.eaDefExists = false
	m.eaDefCreatable = false
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Comment:  stringPtr("old comment"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber()}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", ref, "ProviderConfig")
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	ref := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn"), Ea: ibclient.EA{identity.EAKey: "test-uid-namespaced"}})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "dtc:lbdn/does-not-exist:my-lbdn", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "dtc:lbdn/test1:my-lbdn", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteDTCLBDN) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteDTCLBDN)
	}
}

// TestNamespacedDeleteRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterDeleteRefusesOnForeignIdentity.
func TestNamespacedDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", ref, "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Delete: error = %v, want a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestNamespacedObserveRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterObserveRefusesOnForeignIdentity.
func TestNamespacedObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", ref, "ProviderConfig")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when the resolved object's identity attribute belongs to a different owner, got nil")
	}
	var reuse *identity.HandleReuseError
	if !cperrors.As(err, &reuse) {
		t.Errorf("Observe: error = %v, want a *identity.HandleReuseError", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live record was removed — Observe() must never mutate the backend")
	}
}

// TestNamespacedDeleteSucceedsWhenTrulyAbsent is the namespaced-scope
// counterpart of TestClusterDeleteSucceedsWhenTrulyAbsent.
func TestNamespacedDeleteSucceedsWhenTrulyAbsent(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedDTCLBDN(nsDefault, "my-lbdn", "dtc:lbdn/stale-ref:my-lbdn", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
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
	e := &namespacedExternal{kube: &recordingKubeClient{}, prober: identity.NewProber(), endpoint: t.Name()}
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

// TestLateInitializeStripsIdentityEAFromExtAttrs proves the reserved
// identity key never late-inits into spec.forProvider.extAttrs.
func TestLateInitializeStripsIdentityEAFromExtAttrs(t *testing.T) {
	var priority, persistence, ttl *uint32
	var topology, comment *string
	var useTTL, disable *bool
	var extAttrs map[string]string

	rec := &ibclient.DtcLbdn{
		Ea: ibclient.EA{eaKeyEnv: eaValProd, identity.EAKey: "some-uid"},
	}

	lateInitialize(&priority, &persistence, &topology, &ttl, &useTTL, &comment, &disable, &extAttrs, rec)

	if _, present := extAttrs[identity.EAKey]; present {
		t.Errorf("lateInitialize: extAttrs contains the reserved identity key %q, want it stripped", identity.EAKey)
	}
	if !extAttrsEqual(extAttrs, map[string]string{eaKeyEnv: eaValProd}) {
		t.Errorf("lateInitialize: extAttrs = %v, want {env: prod} (identity key stripped)", extAttrs)
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

// TestLateInitializeDoesNotBackfillTTLWhenUseTTLOff proves that when
// useTtl is false the observed ttl (WAPI's own default, not a value the
// user's config implies) is never written back into spec.forProvider.ttl.
func TestLateInitializeDoesNotBackfillTTLWhenUseTTLOff(t *testing.T) {
	var priority, persistence, ttl *uint32
	var topology, comment *string
	useTTL := boolPtr(false)
	var disable *bool
	var extAttrs map[string]string

	rec := &ibclient.DtcLbdn{
		Ttl:    uint32Ptr(28800),
		UseTtl: boolPtr(false),
	}

	lateInitialize(&priority, &persistence, &topology, &ttl, &useTTL, &comment, &disable, &extAttrs, rec)

	if ttl != nil {
		t.Errorf("lateInitialize: ttl = %v, want nil (useTtl is off, observed ttl is the server's own default, not a user value)", *ttl)
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

// TestIsUpToDateIgnoresTTLWhenUseTTLOff proves the ttl comparison is
// gated on useTtl. When useTtl is false, WAPI ignores the submitted ttl
// and returns its own default (a realistic non-zero value, not 0) on
// every GET — the spec ttl and the observed ttl are unrelated
// quantities, and comparing them unconditionally can never converge.
func TestIsUpToDateIgnoresTTLWhenUseTTLOff(t *testing.T) {
	rec := &ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Ttl:      uint32Ptr(28800),
		UseTtl:   boolPtr(false),
	}

	got := isUpToDate(rec.Name, stringPtr(rec.LbMethod), rec.Patterns, nil, nil, nil, nil, nil, nil, uint32Ptr(0), boolPtr(false), nil, nil, nil, rec)
	if !got {
		t.Error("isUpToDate: want true when useTtl is off and only the server-owned ttl differs, got false (non-convergent drift comparison)")
	}
}

// TestIsUpToDateDetectsUseTTLTransition proves a useTtl true -> false
// transition is still detected as drift even though the value comparison
// is gated off. The flag comparison must be unconditional.
func TestIsUpToDateDetectsUseTTLTransition(t *testing.T) {
	rec := &ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Ttl:      uint32Ptr(300),
		UseTtl:   boolPtr(true),
	}

	got := isUpToDate(rec.Name, stringPtr(rec.LbMethod), rec.Patterns, nil, nil, nil, nil, nil, nil, uint32Ptr(300), boolPtr(false), nil, nil, nil, rec)
	if got {
		t.Error("isUpToDate: want false on a useTtl true -> false transition, got true (drift not detected)")
	}
}

// TestIsUpToDateIgnoresUnsetTypesAgainstServerDefault proves the types
// comparison is skipped when spec.forProvider.types is left unset. WAPI
// assigns a non-empty server default ([A, AAAA]) even when types is
// omitted from Create, so an empty spec value and that default are
// unrelated quantities — comparing them unconditionally can never
// converge (the same class of bug the useTtl-gated ttl comparison above
// fixes for ttl).
func TestIsUpToDateIgnoresUnsetTypesAgainstServerDefault(t *testing.T) {
	rec := &ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Types:    []string{"A", "AAAA"},
	}

	got := isUpToDate(rec.Name, stringPtr(rec.LbMethod), rec.Patterns, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, rec)
	if !got {
		t.Error("isUpToDate: want true when spec.types is unset and only the server-assigned default types differ, got false (non-convergent drift comparison)")
	}
}

// TestIsUpToDateDetectsTypesDriftWhenSet proves the types comparison
// still fires normally once the user has explicitly set spec.types — the
// skip only applies to the unset case.
func TestIsUpToDateDetectsTypesDriftWhenSet(t *testing.T) {
	rec := &ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Types:    []string{"A", "AAAA"},
	}

	got := isUpToDate(rec.Name, stringPtr(rec.LbMethod), rec.Patterns, nil, nil, []string{"A"}, nil, nil, nil, nil, nil, nil, nil, nil, rec)
	if got {
		t.Error("isUpToDate: want false when spec.types is explicitly set and differs from observed types, got true (drift not detected)")
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

// ── identity ladder: Ambiguous match refusal ────────────────────────────

func TestClusterObserveAmbiguousMatchRefusesAndDoesNotMutate(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	uid := "test-uid-cluster"
	ref1 := m.seed(&ibclient.DtcLbdn{Name: stringPtr("lbdn-one"), Ea: ibclient.EA{identity.EAKey: uid}})
	ref2 := m.seed(&ibclient.DtcLbdn{Name: stringPtr("lbdn-two"), Ea: ibclient.EA{identity.EAKey: uid}})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", "")
	meta.SetExternalName(cr, cr.GetName())

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when the identity search matches more than one Grid object, got nil")
	}
	var ambiguous *identity.AmbiguousMatchError
	if !cperrors.As(err, &ambiguous) {
		t.Errorf("Observe: error = %v, want a *identity.AmbiguousMatchError", err)
	}

	m.mu.Lock()
	_, ref1Exists := m.records[ref1]
	_, ref2Exists := m.records[ref2]
	deleteCalls := m.deleteCalls
	createCalls := m.createCalls
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if !ref1Exists || !ref2Exists {
		t.Error("Observe: a live record was removed — Observe() must never mutate the backend, ambiguous or not")
	}
	if deleteCalls != 0 || createCalls != 0 {
		t.Errorf("Observe: deleteCalls=%d createCalls=%d, want 0/0 — an ambiguous match must never trigger a mutating request", deleteCalls, createCalls)
	}
	if eaDefSearchCalls != 0 {
		t.Errorf("Observe: eaDefSearchCalls = %d, want 0 — an AmbiguousMatchError is unrelated to whether the search itself failed and must not probe", eaDefSearchCalls)
	}
}

func TestNamespacedObserveAmbiguousMatchRefusesAndDoesNotMutate(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	uid := "test-uid-namespaced"
	ref1 := m.seed(&ibclient.DtcLbdn{Name: stringPtr("lbdn-one"), Ea: ibclient.EA{identity.EAKey: uid}})
	ref2 := m.seed(&ibclient.DtcLbdn{Name: stringPtr("lbdn-two"), Ea: ibclient.EA{identity.EAKey: uid}})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedDTCLBDN("default", "my-lbdn", "", "ProviderConfig")
	meta.SetExternalName(cr, cr.GetName())

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when the identity search matches more than one Grid object, got nil")
	}
	var ambiguous *identity.AmbiguousMatchError
	if !cperrors.As(err, &ambiguous) {
		t.Errorf("Observe: error = %v, want a *identity.AmbiguousMatchError", err)
	}

	m.mu.Lock()
	_, ref1Exists := m.records[ref1]
	_, ref2Exists := m.records[ref2]
	deleteCalls := m.deleteCalls
	createCalls := m.createCalls
	eaDefSearchCalls := m.eaDefSearchCalls
	m.mu.Unlock()
	if !ref1Exists || !ref2Exists {
		t.Error("Observe: a live record was removed — Observe() must never mutate the backend, ambiguous or not")
	}
	if deleteCalls != 0 || createCalls != 0 {
		t.Errorf("Observe: deleteCalls=%d createCalls=%d, want 0/0 — an ambiguous match must never trigger a mutating request", deleteCalls, createCalls)
	}
	if eaDefSearchCalls != 0 {
		t.Errorf("Observe: eaDefSearchCalls = %d, want 0 — an AmbiguousMatchError is unrelated to whether the search itself failed and must not probe", eaDefSearchCalls)
	}
}

// ── identity ladder: Adopted never reports up to date ───────────────────

func TestClusterObserveAdoptedNeverReportsUpToDate(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		// No identity.EAKey stamped — this object is unowned.
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for an adoptable object, got false")
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false for an adopted (unstamped) object even though every user-facing field matches — otherwise it is never re-stamped")
	}
}

func TestNamespacedObserveAdoptedNeverReportsUpToDate(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedDTCLBDN("default", "my-lbdn", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true for an adoptable object, got false")
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false for an adopted (unstamped) object even though every user-facing field matches — otherwise it is never re-stamped")
	}
}

// ── Create: identity stamp in the wire body, exactly once ───────────────

func TestClusterCreateStampsIdentityEAExactlyOnce(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	rec, ok := m.records[ref]
	createCalls := m.createCalls
	m.mu.Unlock()
	if !ok {
		t.Fatalf("Create: no record captured for ref %q", ref)
	}
	if createCalls != 1 {
		t.Errorf("Create: createCalls = %d, want exactly 1", createCalls)
	}
	if got, present := rec.Ea[identity.EAKey]; !present || got != string(cr.GetUID()) {
		t.Errorf("Create: wire-captured Ea[%q] = %q (present=%v), want %q", identity.EAKey, got, present, cr.GetUID())
	}
}

// TestClusterCreateEmptyUIDFailsWithZeroMutatingRequests: see zoneauth's
// identically-named test. A whitespace-only uid is rejected the same
// way — see TestClusterCreateWhitespaceUIDFailsWithZeroMutatingRequests.
func TestClusterCreateEmptyUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", "")
	cr.UID = types.UID("")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: want a hard error for a blank uid, got nil")
	}

	m.mu.Lock()
	createCalls := m.createCalls
	recordCount := len(m.records)
	m.mu.Unlock()
	if createCalls != 0 || recordCount != 0 {
		t.Errorf("Create: createCalls=%d recordCount=%d, want 0/0 for a blank uid", createCalls, recordCount)
	}
}

// TestClusterCreateWhitespaceUIDFailsWithZeroMutatingRequests: see
// zoneauth's identically-named test for the rationale.
func TestClusterCreateWhitespaceUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", "")
	cr.UID = types.UID("   ")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: want a hard error for a whitespace-only uid, got nil")
	}

	m.mu.Lock()
	createCalls := m.createCalls
	recordCount := len(m.records)
	m.mu.Unlock()
	if createCalls != 0 || recordCount != 0 {
		t.Errorf("Create: createCalls=%d recordCount=%d, want 0/0 for a whitespace-only uid", createCalls, recordCount)
	}
}

// TestClusterUpdateWhitespaceUIDFailsWithZeroMutatingRequests proves the
// Update path rejects a whitespace-only uid the same way Create does —
// updateDtcLbdn's guard trims before comparing, matching
// identity.Resolve's ladder. Without the trim, a whitespace-only uid
// would pass Update's guard and get re-stamped verbatim into the
// object's extensible attributes, while Observe/Delete (which route
// through identity.Resolve) would treat that same object as unowned.
func TestClusterUpdateWhitespaceUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Comment:  stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", ref)
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

// ── rotation: persistence round-trips through a client ──────────────────

func TestClusterObserveRecoversRotatedRefPersistsAcrossReGet(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	cr := newClusterDTCLBDN("my-lbdn", "dtc:lbdn/stale-ref:my-lbdn")
	newRef := m.seed(&ibclient.DtcLbdn{Name: stringPtr("my-lbdn"), Ea: ibclient.EA{identity.EAKey: string(cr.GetUID())}})

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceLateInitialized {
		t.Fatal("Observe: want ResourceLateInitialized=true so the recovered reference is persisted, got false")
	}
	if meta.GetExternalName(cr) != newRef {
		t.Fatalf("Observe: in-memory external-name = %q, want %q", meta.GetExternalName(cr), newRef)
	}

	// Simulate the managed reconciler's post-Observe persistence.
	if err := kube.Update(context.Background(), cr); err != nil {
		t.Fatalf("kube.Update: unexpected error: %v", err)
	}

	fetched := &clusterv1alpha1.DTCLBDN{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: cr.GetName()}, fetched); err != nil {
		t.Fatalf("kube.Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Observe: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

// ── isUpToDate: identity EA never produces a phantom diff ───────────────
//
// isUpToDate has its own identity.Strip call site, separate from
// lateInitializeExtAttrs's. This test targets it directly: a
// spec.extAttrs that already matches the user-facing keys must still
// compare up to date against a live record whose Ea additionally carries
// the reserved identity stamp.
func TestIsUpToDateIgnoresIdentityEA(t *testing.T) {
	rec := &ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Ea:       ibclient.EA{eaKeyEnv: eaValProd, identity.EAKey: "some-uid"},
	}
	name := stringPtr("my-lbdn")
	lbMethod := stringPtr("ROUND_ROBIN")
	patterns := []string{"*.example.com"}

	if !isUpToDate(name, lbMethod, patterns, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{eaKeyEnv: eaValProd}, rec) {
		t.Error("isUpToDate: want true when spec.extAttrs already matches the user-facing keys and the live object's Ea only additionally carries the reserved identity stamp")
	}
}

// ── newEmptyDtcLbdn: the constructor actually passed to Resolve ─────────
//
// internal/clients/identity's own TestNewEmptyCorrectness documents this
// as one of the two packages (alongside dtcpool) whose local newEmpty
// wrapper it does NOT exercise directly. This test closes that gap.
func TestNewEmptyDtcLbdnObjectTypeAndReturnFields(t *testing.T) {
	l := newEmptyDtcLbdn()
	if got := l.ObjectType(); got != "dtc:lbdn" {
		t.Errorf("newEmptyDtcLbdn().ObjectType() = %q, want %q", got, "dtc:lbdn")
	}
	fields := l.ReturnFields()
	found := false
	for _, f := range fields {
		if f == "extattrs" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("newEmptyDtcLbdn().ReturnFields() = %v, want it to contain %q — identity.Resolve reads the identity stamp from the Ea field, which this field populates on GET", fields, "extattrs")
	}
}

// ── status.atProvider mirror retains the identity key (convention 0032) ─

func TestClusterObserveAtProviderExtAttrsRetainsIdentityKey(t *testing.T) {
	m := newMockDtcLbdnServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcLbdn{
		Name:     stringPtr("my-lbdn"),
		LbMethod: "ROUND_ROBIN",
		Patterns: []string{"*.example.com"},
		Ea:       ibclient.EA{eaKeyEnv: eaValProd, identity.EAKey: "test-uid-cluster"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv), prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterDTCLBDN("my-lbdn", ref)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{eaKeyEnv: eaValProd}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got, present := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; !present || got != "test-uid-cluster" {
		t.Errorf("Observe: status.atProvider.extAttrs[%q] = %q (present=%v), want the full-mirror copy to retain the identity stamp", identity.EAKey, got, present)
	}
}
