// Package dnsview unit tests for the DNSView MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI view endpoints,
// PascalCase test names (no underscores), and white-box access to the
// unexported connectors/clients so both scopes can be exercised without
// going through the full Connect() credential bridge on every test.
package dnsview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dnsview/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dnsview/v1alpha1"
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

// newClusterDNSView builds a minimal cluster-scoped DNSView CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterDNSView(crName, externalName string) *clusterv1alpha1.DNSView {
	cr := &clusterv1alpha1.DNSView{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.DNSViewSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.DNSViewParameters{
				Name: stringPtr("my-view"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedDNSView is the namespaced variant of newClusterDNSView.
func newNamespacedDNSView(ns, crName, externalName, pcKind string) *namespacedv1alpha1.DNSView {
	cr := &namespacedv1alpha1.DNSView{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.DNSViewSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.DNSViewParameters{
				Name: stringPtr("my-view"),
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
// mockWapiServer emulates the subset of NIOS WAPI view endpoints exercised
// by the DNSView controller (POST create, GET/PUT/DELETE by _ref). Records
// are marshaled/unmarshaled using the real ibclient.View type so the wire
// format (including the EA {"value": ...} envelope) exactly matches what
// the SDK sends and expects.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.View
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte

	// eaDefExists controls the identity extensible-attribute-definition
	// prerequisite endpoint. Defaults to true via newMockWapiServer.
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

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.View{}, eaDefExists: true}
}

func (m *mockWapiServer) seed(rec *ibclient.View) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		rec.Ref = m.newRefLocked(rec)
	}
	m.records[rec.Ref] = rec
	return rec.Ref
}

func (m *mockWapiServer) newRefLocked(rec *ibclient.View) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	return "view/test" + itoa(m.nextRef) + ":" + name + "/" + boolStr(rec.IsDefault)
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
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

// handler returns an http.Handler implementing the view WAPI surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/view", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.View
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
	// still filter by the name query param. Registered as an exact
	// literal path so Go's ServeMux prefers it over the {ref...}
	// wildcard below for requests to precisely "view" (real _refs always
	// carry additional path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/view", func(w http.ResponseWriter, r *http.Request) {
		uid := r.URL.Query().Get("*" + identity.EAKey)
		name := r.URL.Query().Get("name")

		m.mu.Lock()
		m.searchCalls++
		var matches []ibclient.View
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
		// Mirror the live NIOS Grid Manager pinned at WAPI 2.9.7: the
		// `view` object schema at that version has no edns_udp_size /
		// use_edns_udp_size / last_queried_acl / max_udp_size /
		// use_max_udp_size fields at all, so requesting them in
		// _return_fields is rejected with a 400 (AdmConProtoError:
		// Unknown argument/field).
		for _, f := range strings.Split(r.URL.Query().Get("_return_fields"), ",") {
			if f == "edns_udp_size" || f == "use_edns_udp_size" || f == "last_queried_acl" || f == "max_udp_size" || f == "use_max_udp_size" {
				writeJSON(w, http.StatusBadRequest, map[string]string{
					"Error": "AdmConProtoError: Unknown argument/field: '" + f + "'",
					"code":  "Client.Ibap.Proto",
				})
				return
			}
		}
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
		var incoming ibclient.View
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		// Only mutable fields are ever applied. is_default is read-only
		// (supports=sr) and is never accepted on a PUT — mirroring WAPI's
		// rejection of changes to that field.
		newRef := existing.Ref
		if incoming.Name != nil && (existing.Name == nil || *existing.Name != *incoming.Name) {
			existing.Name = incoming.Name
			newRef = "view/test" + itoa(m.nextRef) + ":" + *incoming.Name + "/" + boolStr(existing.IsDefault)
		}
		existing.Comment = incoming.Comment
		existing.NetworkView = incoming.NetworkView
		existing.Disable = incoming.Disable
		existing.Ea = incoming.Ea
		existing.CustomRootNameServers = incoming.CustomRootNameServers
		if newRef != existing.Ref {
			delete(m.records, existing.Ref)
			existing.Ref = newRef
			m.records[newRef] = existing
		}
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, existing.Ref)
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
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"Error":"boom"}`))
	})
}

// newTestConnector builds an ibclient.IBConnector pointed at the given
// httptest.Server via plain HTTP (no TLS needed — the WapiRequestBuilder
// only switches to HTTPS when hostCfg.Scheme != "http").
func newTestConnector(t *testing.T, srv *httptest.Server) ibclient.IBConnector {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	conn, err := newConnectorWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test connector: %v", err)
	}
	return conn
}

// ── cluster: Observe ────────────────────────────────────────────────────

func TestClusterObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name:    stringPtr("my-view"),
		Comment: stringPtr("hello"),
		Disable: boolPtr(false),
		Ea:      ibclient.EA{"env": "prod", identity.EAKey: "test-uid-cluster"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.Disable = boolPtr(false)
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
	if cr.Status.AtProvider.Name == nil || *cr.Status.AtProvider.Name != "my-view" {
		t.Errorf("AtProvider.Name = %v, want my-view", cr.Status.AtProvider.Name)
	}
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

// TestClusterObserveDoesNotRequestUnsupportedEdnsFields verifies Observe
// never requests edns_udp_size/use_edns_udp_size in the WAPI GET
// return-fields list. The provider is pinned to WAPI 2.9.7, whose `view`
// object schema doesn't define these fields at all — requesting them
// fails every Observe() with a 400 (AdmConProtoError: Unknown
// argument/field), which would otherwise put the resource in a permanent
// ReconcileError loop. The mock server's GET handler rejects these fields
// exactly like the live Grid Manager, so this test fails loudly if the
// fields are ever reintroduced into dnsViewReturnFields.
func TestClusterObserveDoesNotRequestUnsupportedEdnsFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name:    stringPtr("my-view"),
		Comment: stringPtr("hello"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error (edns_udp_size/use_edns_udp_size must not be requested at WAPI 2.9.7): %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true, got false")
	}
}

// TestClusterObserveDoesNotRequestUnsupportedLastQueriedAclField verifies
// Observe never requests last_queried_acl in the WAPI GET return-fields
// list. The provider is pinned to WAPI 2.9.7, whose `view` object schema
// doesn't define this field at all — requesting it fails every Observe()
// with a 400 (AdmConProtoError: Unknown argument/field), which would
// otherwise put the resource in a permanent ReconcileError loop. The mock
// server's GET handler rejects this field exactly like the live Grid
// Manager, so this test fails loudly if the field is ever reintroduced
// into dnsViewReturnFields.
func TestClusterObserveDoesNotRequestUnsupportedLastQueriedAclField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name:    stringPtr("my-view"),
		Comment: stringPtr("hello"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error (last_queried_acl must not be requested at WAPI 2.9.7): %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true, got false")
	}
}

// TestClusterObserveDoesNotRequestUnsupportedMaxUdpSizeField verifies
// Observe never requests max_udp_size/use_max_udp_size in the WAPI GET
// return-fields list. The provider is pinned to WAPI 2.9.7, whose `view`
// object schema doesn't define these fields at all — requesting them
// fails every Observe() with a 400 (AdmConProtoError: Unknown
// argument/field), which would otherwise put the resource in a permanent
// ReconcileError loop. The mock server's GET handler rejects these fields
// exactly like the live Grid Manager, so this test fails loudly if the
// fields are ever reintroduced into dnsViewReturnFields.
func TestClusterObserveDoesNotRequestUnsupportedMaxUdpSizeField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name:    stringPtr("my-view"),
		Comment: stringPtr("hello"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error (max_udp_size/use_max_udp_size must not be requested at WAPI 2.9.7): %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true, got false")
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/does-not-exist:my-view/false")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestClusterObserveStripsIdentityEAFromExtAttrs proves the reserved
// identity key never late-inits into spec.forProvider.extAttrs. The CRD
// schema never includes it, and a CEL rule rejects a user-supplied value
// — back-filling it here would produce a permanent validation failure on
// the very next apply.
func TestClusterObserveStripsIdentityEAFromExtAttrs(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name: stringPtr("my-view"),
		Ea:   ibclient.EA{"env": "prod", identity.EAKey: "test-uid-cluster"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.ExtAttrs = nil // force late-init from the observed EA

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if _, present := cr.Spec.ForProvider.ExtAttrs[identity.EAKey]; present {
		t.Errorf("Observe: spec.forProvider.extAttrs contains the reserved identity key %q, want it stripped", identity.EAKey)
	}
	if !extAttrsEqual(cr.Spec.ForProvider.ExtAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("Observe: spec.forProvider.extAttrs = %v, want {env: prod} (identity key stripped)", cr.Spec.ForProvider.ExtAttrs)
	}
	// The full-mirror AtProvider copy, by contrast, keeps the unstripped
	// map (convention 0032) — this is intentional, not a bug.
	if _, present := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; !present {
		t.Error("Observe: status.atProvider.extAttrs must keep the identity key (full-mirror convention), but it was stripped")
	}
}

// TestObservePreCreateState verifies that Observe runs one identity
// search (not a hard-coded no-op) when the external-name still equals
// the CR's Kubernetes name — the pre-create state for a server-assigned
// external-name strategy. Per ADR-IN-0006 §3 the pre-create guard no
// longer short-circuits: it maps the annotation to "" and lets the
// identity ladder search by uid before concluding ResourceExists:false.
func TestObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "") // external-name unset
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
		t.Error("Observe: want the identity ladder to search by uid even in the pre-create state (ADR-IN-0006 §3), got zero search calls")
	}
}

func TestClusterObserveServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/test1:my-view/false")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/test1:my-view/false")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref/name and every other field at
// its Go zero value (nil pointers, empty strings, a nil Ea map, nil
// slices) must not panic and must produce a valid observation with
// nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view")})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

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
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.CustomRootNameServers != nil {
		t.Errorf("AtProvider.CustomRootNameServers = %v, want nil", ap.CustomRootNameServers)
	}
	if ap.ScavengingSettings != nil {
		t.Errorf("AtProvider.ScavengingSettings = %v, want nil", ap.ScavengingSettings)
	}
}

// ── cluster: is_default (immutable field) ────────────────────────────────

// TestIsUpToDateIgnoresIsDefault verifies is_default (read-only, no
// ForProvider representation) is excluded from the mutable-field
// comparison — a view whose spec matches every mutable field must report
// ResourceUpToDate=true regardless of its observed is_default value.
func TestIsUpToDateIgnoresIsDefault(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name:      stringPtr("default"),
		IsDefault: true,
		Ea:        ibclient.EA{identity.EAKey: "test-uid-cluster"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Name = stringPtr("default")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true (is_default excluded from comparison), got false")
	}
	if cr.Status.AtProvider.IsDefault == nil || !*cr.Status.AtProvider.IsDefault {
		t.Errorf("AtProvider.IsDefault = %v, want true (still mirrored in status)", cr.Status.AtProvider.IsDefault)
	}
}

// TestUpdateDoesNotSendImmutableField asserts the PUT body never carries
// is_default — it has no ForProvider field at all, so buildView can never
// emit it, but this pins that guarantee against regression.
func TestUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("default"), IsDefault: true})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Name = stringPtr("default")
	cr.Spec.ForProvider.Comment = stringPtr("updated")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	body := m.lastUpdateBody
	m.mu.Unlock()

	var sent map[string]interface{}
	if err := json.Unmarshal(body, &sent); err != nil {
		t.Fatalf("cannot unmarshal PUT body: %v", err)
	}
	if _, present := sent["is_default"]; present {
		t.Errorf("Update: PUT body contains immutable field is_default, want absent. body=%s", body)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestClusterCreateCapturesServerAssignedRef(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "")
	cr.Spec.ForProvider.Comment = stringPtr("created by test")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	ref := meta.GetExternalName(cr)
	m.mu.Lock()
	rec, ok := m.records[ref]
	m.mu.Unlock()
	if !ok {
		t.Fatalf("Create: seeded record not found for ref %q", ref)
	}
	if rec.Name == nil || *rec.Name != "my-view" {
		t.Errorf("Create: Name = %v, want my-view", rec.Name)
	}
	if rec.Comment == nil || *rec.Comment != "created by test" {
		t.Errorf("Create: Comment = %v, want 'created by test'", rec.Comment)
	}
}

// TestClusterCreateError verifies a WAPI POST failure (500) surfaces as a
// wrapped error and leaves the external-name annotation unset (no ref was
// ever assigned).
func TestClusterCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q after failed Create, want empty", got)
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name:    stringPtr("my-view"),
		Comment: stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	rec := m.records[ref]
	m.mu.Unlock()
	if rec.Comment == nil || *rec.Comment != "new comment" {
		t.Errorf("Update: Comment = %v, want 'new comment'", rec.Comment)
	}
}

// TestClusterUpdateRefChangesOnRename pins the _ref-unstable behavior: PUT
// a name change and confirm the controller re-reads the new _ref from the
// response and refreshes the external-name annotation.
func TestClusterUpdateRefChangesOnRename(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("old-name")})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Name = stringPtr("new-name")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == ref {
		t.Error("Update: external-name unchanged after rename, want the new _ref")
	}
	m.mu.Lock()
	_, oldStillExists := m.records[ref]
	newRec, newExists := m.records[got]
	m.mu.Unlock()
	if oldStillExists {
		t.Error("Update: old _ref still present in the WAPI after rename")
	}
	if !newExists || newRec.Name == nil || *newRec.Name != "new-name" {
		t.Errorf("Update: new ref record = %v, want Name=new-name", newRec)
	}
}

// TestClusterUpdateError verifies a WAPI PUT failure (500) surfaces as a
// wrapped error and leaves the external-name annotation unchanged.
func TestClusterUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/test1:my-view/false")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := meta.GetExternalName(cr); got != "view/test1:my-view/false" {
		t.Errorf("Update: external-name = %q after failed Update, want unchanged", got)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view"), Ea: ibclient.EA{identity.EAKey: "test-uid-cluster"}})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

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

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/does-not-exist:my-view/false")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Errorf("Delete: expected nil error for already-deleted resource (404), got %v", err)
	}
}

// TestClusterDeleteError verifies a non-404 WAPI DELETE failure (500)
// surfaces as a wrapped error rather than being swallowed like a 404.
func TestClusterDeleteError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/test1:my-view/false")

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
}

// TestClusterDeleteRefusesOnForeignIdentity verifies the identity ladder's
// handle-reuse refusal: the stored _ref still resolves, but its stamped
// identity attribute belongs to a different managed resource. Deleting it
// would destroy someone else's object, so Delete() must refuse and leave
// the record in place.
func TestClusterDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view")})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	cr := newClusterDNSView("my-dnsview", "view/stale-ref:my-view/false")
	liveRef := m.seed(&ibclient.View{Name: stringPtr("my-view"), Ea: ibclient.EA{identity.EAKey: string(cr.GetUID())}})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "view/stale-ref:my-view/false")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
	}
}

// TestClusterDeleteProtectsWellKnownDefaultView verifies that a DNSView CR
// whose observed/desired name is one of the three well-known views
// (default/External/Internal) never issues a WAPI DELETE — protecting the
// live Grid Manager from a well-known view being wiped out by an
// accidental `kubectl delete`.
func TestClusterDeleteProtectsWellKnownDefaultView(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("default"), IsDefault: true})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)
	cr.Spec.ForProvider.Name = stringPtr("default")
	cr.Status.AtProvider.Name = stringPtr("default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.records[ref]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: well-known default view was deleted from the WAPI, want protected (still present)")
	}
}

func TestClusterDeleteProtectsExternalAndInternal(t *testing.T) {
	for _, name := range []string{"External", "Internal"} {
		name := name
		t.Run(name, func(t *testing.T) {
			m := newMockWapiServer()
			srv := httptest.NewServer(m.handler())
			defer srv.Close()

			ref := m.seed(&ibclient.View{Name: stringPtr(name)})

			e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
			cr := newClusterDNSView("my-dnsview", ref)
			cr.Spec.ForProvider.Name = stringPtr(name)
			cr.Status.AtProvider.Name = stringPtr(name)

			if _, err := e.Delete(context.Background(), cr); err != nil {
				t.Fatalf("Delete: unexpected error: %v", err)
			}

			m.mu.Lock()
			_, stillExists := m.records[ref]
			m.mu.Unlock()
			if !stillExists {
				t.Errorf("Delete: well-known view %q was deleted from the WAPI, want protected", name)
			}
		})
	}
}

// TestClusterObserveRefusesOnForeignIdentity verifies the Observe()-side
// half of handle-reuse refusal: crossplane-runtime's managed reconciler
// calls Observe() before Delete() on the deletion path, and if Observe()
// silently adopted a foreign object it would let the next Update/Delete
// mutate or destroy someone else's record.
func TestClusterObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

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

// ── cluster: Connect ──────────────────────────────────────────────────────

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

	cr := newClusterDNSView("my-dnsview", "")
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

	cr := newClusterDNSView("my-dnsview", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

func TestClusterDisconnectIsNoop(t *testing.T) {
	e := &clusterExternal{kube: &recordingKubeClient{}}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect: unexpected error: %v", err)
	}
}

// ── namespaced: Observe / Create / Update / Delete ───────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view")})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "view/does-not-exist:my-view/false", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(cr); got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateError verifies a WAPI POST failure (500) surfaces as
// a wrapped error and leaves the external-name annotation unset.
func TestNamespacedCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q after failed Create, want empty", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view"), Comment: stringPtr("old")})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	rec := m.records[ref]
	m.mu.Unlock()
	if rec.Comment == nil || *rec.Comment != "new" {
		t.Errorf("Update: Comment = %v, want 'new'", rec.Comment)
	}
}

// TestNamespacedUpdateError verifies a WAPI PUT failure (500) surfaces as a
// wrapped error and leaves the external-name annotation unchanged.
func TestNamespacedUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "view/test1:my-view/false", "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := meta.GetExternalName(cr); got != "view/test1:my-view/false" {
		t.Errorf("Update: external-name = %q after failed Update, want unchanged", got)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view"), Ea: ibclient.EA{identity.EAKey: "test-uid-namespaced"}})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "view/does-not-exist:my-view/false", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Errorf("Delete: expected nil error for already-deleted resource (404), got %v", err)
	}
}

// TestNamespacedDeleteError verifies a non-404 WAPI DELETE failure (500)
// surfaces as a wrapped error rather than being swallowed like a 404.
func TestNamespacedDeleteError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "view/test1:my-view/false", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
}

// TestNamespacedDeleteRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterDeleteRefusesOnForeignIdentity.
func TestNamespacedDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{Name: stringPtr("my-view"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "view/stale-ref:my-view/false", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
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

	cr := newNamespacedDNSView(ns, "my-dnsview", "", "ProviderConfig")
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

	cr := newNamespacedDNSView("app-ns", "my-dnsview", "", "ClusterProviderConfig")
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

	cr := newNamespacedDNSView("default", "my-dnsview", "", "SomeOtherKind")
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

func TestIsNotFoundClassifiesFormattedStatus(t *testing.T) {
	err := &formattedWapiError{msg: "WAPI request error: 404('Not Found')\nContents:\n{}\n"}
	if !isNotFound(err) {
		t.Error("isNotFound(formatted 404) = false, want true")
	}
}

func TestIsNotFoundFalseForNil(t *testing.T) {
	if isNotFound(nil) {
		t.Error("isNotFound(nil) = true, want false")
	}
}

// formattedWapiError mimics the plain-string error the SDK's
// getHTTPResponseError constructs for non-404 statuses.
type formattedWapiError struct{ msg string }

func (e *formattedWapiError) Error() string { return e.msg }

func TestIsWellKnownDNSViewName(t *testing.T) {
	cases := []struct {
		name *string
		want bool
	}{
		{stringPtr("default"), true},
		{stringPtr("External"), true},
		{stringPtr("Internal"), true},
		{stringPtr("custom-view"), false},
		{nil, false},
	}
	for _, c := range cases {
		if got := isWellKnownDNSViewName(c.name); got != c.want {
			t.Errorf("isWellKnownDNSViewName(%v) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestNameServerValuesRoundTrip(t *testing.T) {
	in := []nameServerValue{{
		Address:                      stringPtr("10.0.0.1"),
		Name:                         stringPtr("ns1.example.com"),
		SharedWithMsParentDelegation: boolPtr(true),
		Stealth:                      boolPtr(false),
		TsigKey:                      stringPtr("key"),
		TsigKeyAlg:                   stringPtr("hmac-sha256"),
		TsigKeyName:                  stringPtr("keyname"),
		UseTsigKeyName:               boolPtr(true),
	}}
	sdk := nameServerValuesToSDK(in)
	back := nameServerValuesFromSDK(sdk)
	if len(back) != 1 || *back[0].Address != *in[0].Address || *back[0].Name != *in[0].Name {
		t.Errorf("NameServer round-trip mismatch: got %+v, want %+v", back, in)
	}
}

func TestIsUpToDateExtAttrsMismatch(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), ExtAttrs: map[string]string{"env": "prod"}}
	observed := dnsViewFields{Name: stringPtr("v"), ExtAttrs: map[string]string{"env": "dev"}}
	if isUpToDate(desired, observed) {
		t.Error("isUpToDate: want false on ExtAttrs mismatch, got true")
	}
}

func TestIsUpToDateNestedListMismatch(t *testing.T) {
	// Forwarders is gated by UseForwarders (drift is only checked while
	// the flag is on) — set it true on both sides so this is a genuine
	// forwarders mismatch, not a flag-off no-op.
	desired := dnsViewFields{Name: stringPtr("v"), UseForwarders: boolPtr(true), Forwarders: []string{"8.8.8.8"}}
	observed := dnsViewFields{Name: stringPtr("v"), UseForwarders: boolPtr(true), Forwarders: []string{"1.1.1.1"}}
	if isUpToDate(desired, observed) {
		t.Error("isUpToDate: want false on Forwarders mismatch, got true")
	}
}

func TestLateInitializeBackfillsServerDefaults(t *testing.T) {
	// NotifyDelay carries no use flag in the WAPI view object (unlike
	// LameTTL/MaxCacheTTL/MaxNcacheTTL, which are use-flag-gated override
	// fields — see TestLateInitializeSkipsGatedValueWhenFlagOff), so it
	// back-fills unconditionally.
	desired := dnsViewFields{Name: stringPtr("v")}
	observed := dnsViewFields{
		Name:        stringPtr("v"),
		Comment:     stringPtr("server default"),
		NetworkView: stringPtr("default"),
		NotifyDelay: uint32Ptr(600),
	}

	got, changed := lateInitializeFields(desired, observed)
	if !changed {
		t.Error("lateInitializeFields: want changed=true, got false")
	}
	if got.Comment == nil || *got.Comment != "server default" {
		t.Errorf("Comment = %v, want 'server default'", got.Comment)
	}
	if got.NetworkView == nil || *got.NetworkView != "default" {
		t.Errorf("NetworkView = %v, want 'default'", got.NetworkView)
	}
	if got.NotifyDelay == nil || *got.NotifyDelay != 600 {
		t.Errorf("NotifyDelay = %v, want 600", got.NotifyDelay)
	}
}

// TestLateInitializeSkipsGatedValueWhenFlagOff proves lateInitializeFields
// does not back-fill LameTTL from the observed zone/grid default while
// UseLameTTL is off — the value is server-owned, not something the user's
// spec implies, so writing it into spec would misrepresent intent.
func TestLateInitializeSkipsGatedValueWhenFlagOff(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), UseLameTTL: boolPtr(false)}
	observed := dnsViewFields{
		Name:       stringPtr("v"),
		UseLameTTL: boolPtr(false),
		LameTTL:    uint32Ptr(600), // realistic non-zero zone default, not 0
	}

	got, _ := lateInitializeFields(desired, observed)
	if got.LameTTL != nil {
		t.Errorf("lateInitializeFields: LameTTL = %v, want nil (UseLameTTL is off, observed value is the zone default)", *got.LameTTL)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), Comment: stringPtr("user set")}
	observed := dnsViewFields{Name: stringPtr("v"), Comment: stringPtr("server value")}

	got, changed := lateInitializeFields(desired, observed)
	if changed {
		t.Error("lateInitializeFields: want changed=false when nothing to back-fill for Comment, got true")
	}
	if got.Comment == nil || *got.Comment != "user set" {
		t.Errorf("Comment = %v, want 'user set' (must not be overwritten)", got.Comment)
	}
}

// ── full-field-mirror coverage (nested lists/singles) ────────────────────

// TestClusterObserveFullFieldMirror seeds a WAPI response exercising every
// nested list/single field (custom root name servers, DNSSEC trusted
// keys, address-ACL lists, fixed-RRset-order FQDNs, response rate
// limiting, scavenging settings with a schedule and both expression
// lists, and a sortlist) and confirms Observe mirrors every one of them
// into AtProvider without panicking — the full-mirror AtProvider
// convention applied to DNSView's deepest nesting.
func TestClusterObserveFullFieldMirror(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name: stringPtr("full-view"),
		CustomRootNameServers: []ibclient.NameServer{
			{Address: "10.0.0.1", Name: "ns1.example.com"},
		},
		DnssecTrustedKeys: []*ibclient.Dnssectrustedkey{
			{Fqdn: "example.com", Algorithm: "RSASHA256", Key: "abc123"},
		},
		FilterAaaaList: []*ibclient.Addressac{
			{Address: "192.0.2.0/24", Permission: "ALLOW"},
		},
		MatchClients:      []*ibclient.Addressac{{Address: "198.51.100.0/24", Permission: "ALLOW"}},
		MatchDestinations: []*ibclient.Addressac{{Address: "203.0.113.0/24", Permission: "DENY"}},
		FixedRrsetOrderFqdns: []*ibclient.GridDnsFixedrrsetorderfqdn{
			{Fqdn: "svc.example.com", RecordType: "A"},
		},
		ResponseRateLimiting: &ibclient.GridResponseratelimiting{
			EnableRrl: true, ResponsesPerSecond: 5, Window: 1, Slip: 2,
		},
		ScavengingSettings: &ibclient.SettingScavenging{
			EnableScavenging: true,
			ScavengingSchedule: &ibclient.SettingSchedule{
				Weekdays: []string{"Monday"}, TimeZone: "UTC", Frequency: "weekly", Every: 1,
			},
			ExpressionList:   []*ibclient.Expressionop{{Op: "AND", Op1: "a", Op1Type: "STRING"}},
			EaExpressionList: []*ibclient.Eaexpressionop{{Op: "AND", Op1: "b", Op1Type: "STRING"}},
		},
		Sortlist: []*ibclient.Sortlist{
			{Address: "10.0.0.0/8", MatchList: []string{"10.0.0.1"}},
		},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true")
	}

	ap := cr.Status.AtProvider
	if len(ap.CustomRootNameServers) != 1 || ap.CustomRootNameServers[0].Address == nil || *ap.CustomRootNameServers[0].Address != "10.0.0.1" {
		t.Errorf("AtProvider.CustomRootNameServers = %+v, want one entry with Address=10.0.0.1", ap.CustomRootNameServers)
	}
	if len(ap.DnssecTrustedKeys) != 1 || ap.DnssecTrustedKeys[0].Fqdn == nil || *ap.DnssecTrustedKeys[0].Fqdn != "example.com" {
		t.Errorf("AtProvider.DnssecTrustedKeys = %+v, want one entry with Fqdn=example.com", ap.DnssecTrustedKeys)
	}
	if len(ap.FilterAaaaList) != 1 || len(ap.MatchClients) != 1 || len(ap.MatchDestinations) != 1 {
		t.Errorf("AtProvider address-ACL lists not fully mirrored: %+v", ap)
	}
	if len(ap.FixedRrsetOrderFqdns) != 1 || ap.FixedRrsetOrderFqdns[0].Fqdn == nil || *ap.FixedRrsetOrderFqdns[0].Fqdn != "svc.example.com" {
		t.Errorf("AtProvider.FixedRrsetOrderFqdns = %+v, want one entry with Fqdn=svc.example.com", ap.FixedRrsetOrderFqdns)
	}
	if ap.ResponseRateLimiting == nil || ap.ResponseRateLimiting.ResponsesPerSecond == nil || *ap.ResponseRateLimiting.ResponsesPerSecond != 5 {
		t.Errorf("AtProvider.ResponseRateLimiting = %+v, want ResponsesPerSecond=5", ap.ResponseRateLimiting)
	}
	if ap.ScavengingSettings == nil || ap.ScavengingSettings.EnableScavenging == nil || !*ap.ScavengingSettings.EnableScavenging {
		t.Fatalf("AtProvider.ScavengingSettings = %+v, want EnableScavenging=true", ap.ScavengingSettings)
	}
	if ap.ScavengingSettings.ScavengingSchedule == nil || ap.ScavengingSettings.ScavengingSchedule.TimeZone == nil || *ap.ScavengingSettings.ScavengingSchedule.TimeZone != "UTC" {
		t.Errorf("AtProvider.ScavengingSettings.ScavengingSchedule = %+v, want TimeZone=UTC", ap.ScavengingSettings.ScavengingSchedule)
	}
	if len(ap.ScavengingSettings.ExpressionList) != 1 || len(ap.ScavengingSettings.EaExpressionList) != 1 {
		t.Errorf("AtProvider.ScavengingSettings expression lists not mirrored: %+v", ap.ScavengingSettings)
	}
	if len(ap.Sortlist) != 1 || ap.Sortlist[0].Address == nil || *ap.Sortlist[0].Address != "10.0.0.0/8" {
		t.Errorf("AtProvider.Sortlist = %+v, want one entry with Address=10.0.0.0/8", ap.Sortlist)
	}

	// Round-trip: Update() must be able to re-send everything Observe just
	// populated into spec.ForProvider (via late-init) without panicking —
	// proves the Cluster<->bag<->SDK conversions compose in both
	// directions for every nested type, not just the read path above.
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error round-tripping full field set: %v", err)
	}
}

// TestNamespacedObserveFullFieldMirror is the namespaced-scope counterpart
// of TestClusterObserveFullFieldMirror — same nested-field coverage, using
// the namespaced CRD conversion path (fieldsFromNamespacedParams /
// namespacedObservationFromFields) instead of the cluster one. Also
// exercises the SDK's epoch-seconds RecurringTime field, which the
// cluster-side test above leaves unset.
func TestNamespacedObserveFullFieldMirror(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name: stringPtr("full-view-ns"),
		CustomRootNameServers: []ibclient.NameServer{
			{Address: "10.0.0.2", Name: "ns2.example.com"},
		},
		DnssecTrustedKeys: []*ibclient.Dnssectrustedkey{
			{Fqdn: "example.org", Algorithm: "RSASHA256", Key: "def456"},
		},
		FilterAaaaList:    []*ibclient.Addressac{{Address: "192.0.2.0/24", Permission: "ALLOW"}},
		MatchClients:      []*ibclient.Addressac{{Address: "198.51.100.0/24", Permission: "ALLOW"}},
		MatchDestinations: []*ibclient.Addressac{{Address: "203.0.113.0/24", Permission: "DENY"}},
		FixedRrsetOrderFqdns: []*ibclient.GridDnsFixedrrsetorderfqdn{
			{Fqdn: "svc.example.org", RecordType: "A"},
		},
		ResponseRateLimiting: &ibclient.GridResponseratelimiting{
			EnableRrl: true, ResponsesPerSecond: 10, Window: 2, Slip: 1,
		},
		ScavengingSettings: &ibclient.SettingScavenging{
			EnableScavenging: true,
			ScavengingSchedule: &ibclient.SettingSchedule{
				Weekdays:      []string{"Tuesday"},
				TimeZone:      "UTC",
				Frequency:     "weekly",
				Every:         1,
				RecurringTime: &ibclient.UnixTime{Time: time.Unix(1700000000, 0)},
			},
			ExpressionList:   []*ibclient.Expressionop{{Op: "AND", Op1: "a", Op1Type: "STRING"}},
			EaExpressionList: []*ibclient.Eaexpressionop{{Op: "AND", Op1: "b", Op1Type: "STRING"}},
		},
		Sortlist: []*ibclient.Sortlist{
			{Address: "10.0.0.0/8", MatchList: []string{"10.0.0.2"}},
		},
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true")
	}

	ap := cr.Status.AtProvider
	if len(ap.CustomRootNameServers) != 1 || ap.CustomRootNameServers[0].Name == nil || *ap.CustomRootNameServers[0].Name != "ns2.example.com" {
		t.Errorf("AtProvider.CustomRootNameServers = %+v, want one entry with Name=ns2.example.com", ap.CustomRootNameServers)
	}
	if len(ap.DnssecTrustedKeys) != 1 {
		t.Errorf("AtProvider.DnssecTrustedKeys = %+v, want one entry", ap.DnssecTrustedKeys)
	}
	if len(ap.FilterAaaaList) != 1 || len(ap.MatchClients) != 1 || len(ap.MatchDestinations) != 1 {
		t.Errorf("AtProvider address-ACL lists not fully mirrored: %+v", ap)
	}
	if len(ap.FixedRrsetOrderFqdns) != 1 {
		t.Errorf("AtProvider.FixedRrsetOrderFqdns = %+v, want one entry", ap.FixedRrsetOrderFqdns)
	}
	if ap.ResponseRateLimiting == nil || ap.ResponseRateLimiting.ResponsesPerSecond == nil || *ap.ResponseRateLimiting.ResponsesPerSecond != 10 {
		t.Errorf("AtProvider.ResponseRateLimiting = %+v, want ResponsesPerSecond=10", ap.ResponseRateLimiting)
	}
	if ap.ScavengingSettings == nil || ap.ScavengingSettings.ScavengingSchedule == nil || ap.ScavengingSettings.ScavengingSchedule.RecurringTime == nil || *ap.ScavengingSettings.ScavengingSchedule.RecurringTime != 1700000000 {
		t.Errorf("AtProvider.ScavengingSettings.ScavengingSchedule.RecurringTime = %+v, want 1700000000", ap.ScavengingSettings)
	}
	if len(ap.Sortlist) != 1 {
		t.Errorf("AtProvider.Sortlist = %+v, want one entry", ap.Sortlist)
	}

	// Round-trip through Update() — same rationale as the cluster-scope test.
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error round-tripping full field set: %v", err)
	}
}

// ── use-flag/value pair gating (isUpToDate can never see false drift when
// a use flag is off) ────────────────────────────────────────────────────
//
// The View object is compared as isUpToDate(desired, observed dnsViewFields)
// — a single internal struct on both sides — which is exactly the shape a
// mechanical "does this file contain rec.UseX alongside rec.X from a
// different SDK type" scan cannot infer pairs from. Every field this
// provider's own SDK dependency documents as "Use flag for: X" is
// enumerated here by hand instead.

// TestIsUpToDateIgnoresGatedValueWhenFlagOff is a table-driven regression
// test for every use-flag/value pair in the DNSView field comparator
// table. Each case seeds the observed side with a realistic non-zero server
// default while the corresponding use flag is off on both sides (so the
// flag's own unconditional comparator does not itself report drift), and
// asserts isUpToDate still reports convergence — proving the value
// comparison is gated, not compared unconditionally. A test that seeded the
// observed value with a zero value would pass against the broken,
// unguarded code and prove nothing.
func TestIsUpToDateIgnoresGatedValueWhenFlagOff(t *testing.T) {
	base := func() dnsViewFields { return dnsViewFields{Name: stringPtr("v")} }

	cases := []struct {
		name     string
		desired  dnsViewFields
		observed dnsViewFields
	}{
		{
			name:    "UseBlacklist/BlacklistAction",
			desired: func() dnsViewFields { f := base(); f.UseBlacklist = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseBlacklist = boolPtr(false)
				f.BlacklistAction = strPtrOrNil("REDIRECT")
				return f
			}(),
		},
		{
			name:    "UseBlacklist/BlacklistLogQuery",
			desired: func() dnsViewFields { f := base(); f.UseBlacklist = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseBlacklist = boolPtr(false)
				f.BlacklistLogQuery = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseBlacklist/BlacklistRedirectAddresses",
			desired: func() dnsViewFields { f := base(); f.UseBlacklist = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseBlacklist = boolPtr(false)
				f.BlacklistRedirectAddresses = []string{"10.0.0.1"}
				return f
			}(),
		},
		{
			name:    "UseBlacklist/BlacklistRedirectTTL",
			desired: func() dnsViewFields { f := base(); f.UseBlacklist = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseBlacklist = boolPtr(false)
				f.BlacklistRedirectTTL = uint32Ptr(3600)
				return f
			}(),
		},
		{
			name:    "UseBlacklist/BlacklistRulesets",
			desired: func() dnsViewFields { f := base(); f.UseBlacklist = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseBlacklist = boolPtr(false)
				f.BlacklistRulesets = []string{"ruleset1"}
				return f
			}(),
		},
		{
			name:    "UseBlacklist/EnableBlacklist",
			desired: func() dnsViewFields { f := base(); f.UseBlacklist = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseBlacklist = boolPtr(false)
				f.EnableBlacklist = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseRootNameServer/RootNameServerType",
			desired: func() dnsViewFields { f := base(); f.UseRootNameServer = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseRootNameServer = boolPtr(false)
				f.RootNameServerType = strPtrOrNil("INTERNET")
				return f
			}(),
		},
		{
			name:    "UseDdnsForceCreationTimestampUpdate/DdnsForceCreationTimestampUpdate",
			desired: func() dnsViewFields { f := base(); f.UseDdnsForceCreationTimestampUpdate = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDdnsForceCreationTimestampUpdate = boolPtr(false)
				f.DdnsForceCreationTimestampUpdate = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseDdnsPrincipalSecurity/DdnsPrincipalGroup",
			desired: func() dnsViewFields { f := base(); f.UseDdnsPrincipalSecurity = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDdnsPrincipalSecurity = boolPtr(false)
				f.DdnsPrincipalGroup = strPtrOrNil("group1")
				return f
			}(),
		},
		{
			name:    "UseDdnsPrincipalSecurity/DdnsPrincipalTracking",
			desired: func() dnsViewFields { f := base(); f.UseDdnsPrincipalSecurity = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDdnsPrincipalSecurity = boolPtr(false)
				f.DdnsPrincipalTracking = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseDdnsPrincipalSecurity/DdnsRestrictSecure",
			desired: func() dnsViewFields { f := base(); f.UseDdnsPrincipalSecurity = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDdnsPrincipalSecurity = boolPtr(false)
				f.DdnsRestrictSecure = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseDdnsPatternsRestriction/DdnsRestrictPatterns",
			desired: func() dnsViewFields { f := base(); f.UseDdnsPatternsRestriction = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDdnsPatternsRestriction = boolPtr(false)
				f.DdnsRestrictPatterns = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseDdnsPatternsRestriction/DdnsRestrictPatternsList",
			desired: func() dnsViewFields { f := base(); f.UseDdnsPatternsRestriction = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDdnsPatternsRestriction = boolPtr(false)
				f.DdnsRestrictPatternsList = []string{"*.example.com"}
				return f
			}(),
		},
		{
			name:    "UseDdnsRestrictProtected/DdnsRestrictProtected",
			desired: func() dnsViewFields { f := base(); f.UseDdnsRestrictProtected = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDdnsRestrictProtected = boolPtr(false)
				f.DdnsRestrictProtected = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseDdnsRestrictStatic/DdnsRestrictStatic",
			desired: func() dnsViewFields { f := base(); f.UseDdnsRestrictStatic = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDdnsRestrictStatic = boolPtr(false)
				f.DdnsRestrictStatic = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseDns64/Dns64Enabled",
			desired: func() dnsViewFields { f := base(); f.UseDns64 = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDns64 = boolPtr(false)
				f.Dns64Enabled = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseDns64/Dns64Groups",
			desired: func() dnsViewFields { f := base(); f.UseDns64 = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDns64 = boolPtr(false)
				f.Dns64Groups = []string{"group1"}
				return f
			}(),
		},
		{
			name:    "UseDnssec/DnssecEnabled",
			desired: func() dnsViewFields { f := base(); f.UseDnssec = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDnssec = boolPtr(false)
				f.DnssecEnabled = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseDnssec/DnssecExpiredSignaturesEnabled",
			desired: func() dnsViewFields { f := base(); f.UseDnssec = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDnssec = boolPtr(false)
				f.DnssecExpiredSignaturesEnabled = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseDnssec/DnssecValidationEnabled",
			desired: func() dnsViewFields { f := base(); f.UseDnssec = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseDnssec = boolPtr(false)
				f.DnssecValidationEnabled = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseFixedRrsetOrderFqdns/EnableFixedRrsetOrderFqdns",
			desired: func() dnsViewFields { f := base(); f.UseFixedRrsetOrderFqdns = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseFixedRrsetOrderFqdns = boolPtr(false)
				f.EnableFixedRrsetOrderFqdns = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseFilterAaaa/FilterAaaa",
			desired: func() dnsViewFields { f := base(); f.UseFilterAaaa = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseFilterAaaa = boolPtr(false)
				f.FilterAaaa = strPtrOrNil("BREAK_DNSSEC")
				return f
			}(),
		},
		{
			name:    "UseForwarders/ForwardOnly",
			desired: func() dnsViewFields { f := base(); f.UseForwarders = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseForwarders = boolPtr(false)
				f.ForwardOnly = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseForwarders/Forwarders",
			desired: func() dnsViewFields { f := base(); f.UseForwarders = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseForwarders = boolPtr(false)
				f.Forwarders = []string{"8.8.8.8"}
				return f
			}(),
		},
		{
			name:     "UseLameTTL/LameTTL",
			desired:  func() dnsViewFields { f := base(); f.UseLameTTL = boolPtr(false); return f }(),
			observed: func() dnsViewFields { f := base(); f.UseLameTTL = boolPtr(false); f.LameTTL = uint32Ptr(600); return f }(),
		},
		{
			name:    "UseMaxCacheTTL/MaxCacheTTL",
			desired: func() dnsViewFields { f := base(); f.UseMaxCacheTTL = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseMaxCacheTTL = boolPtr(false)
				f.MaxCacheTTL = uint32Ptr(86400)
				return f
			}(),
		},
		{
			name:    "UseMaxNcacheTTL/MaxNcacheTTL",
			desired: func() dnsViewFields { f := base(); f.UseMaxNcacheTTL = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseMaxNcacheTTL = boolPtr(false)
				f.MaxNcacheTTL = uint32Ptr(10800)
				return f
			}(),
		},
		{
			name:    "UseNxdomainRedirect/NxdomainLogQuery",
			desired: func() dnsViewFields { f := base(); f.UseNxdomainRedirect = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseNxdomainRedirect = boolPtr(false)
				f.NxdomainLogQuery = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseNxdomainRedirect/NxdomainRedirect",
			desired: func() dnsViewFields { f := base(); f.UseNxdomainRedirect = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseNxdomainRedirect = boolPtr(false)
				f.NxdomainRedirect = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseNxdomainRedirect/NxdomainRedirectAddresses",
			desired: func() dnsViewFields { f := base(); f.UseNxdomainRedirect = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseNxdomainRedirect = boolPtr(false)
				f.NxdomainRedirectAddresses = []string{"10.0.0.1"}
				return f
			}(),
		},
		{
			name:    "UseNxdomainRedirect/NxdomainRedirectAddressesV6",
			desired: func() dnsViewFields { f := base(); f.UseNxdomainRedirect = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseNxdomainRedirect = boolPtr(false)
				f.NxdomainRedirectAddressesV6 = []string{"::1"}
				return f
			}(),
		},
		{
			name:    "UseNxdomainRedirect/NxdomainRedirectTTL",
			desired: func() dnsViewFields { f := base(); f.UseNxdomainRedirect = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseNxdomainRedirect = boolPtr(false)
				f.NxdomainRedirectTTL = uint32Ptr(60)
				return f
			}(),
		},
		{
			name:    "UseNxdomainRedirect/NxdomainRulesets",
			desired: func() dnsViewFields { f := base(); f.UseNxdomainRedirect = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseNxdomainRedirect = boolPtr(false)
				f.NxdomainRulesets = []string{"ruleset1"}
				return f
			}(),
		},
		{
			name:    "UseRecursion/Recursion",
			desired: func() dnsViewFields { f := base(); f.UseRecursion = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseRecursion = boolPtr(false)
				f.Recursion = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseRpzDropIPRule/RpzDropIPRuleEnabled",
			desired: func() dnsViewFields { f := base(); f.UseRpzDropIPRule = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseRpzDropIPRule = boolPtr(false)
				f.RpzDropIPRuleEnabled = boolPtr(true)
				return f
			}(),
		},
		{
			name:    "UseRpzDropIPRule/RpzDropIPRuleMinPrefixLengthIPv4",
			desired: func() dnsViewFields { f := base(); f.UseRpzDropIPRule = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseRpzDropIPRule = boolPtr(false)
				f.RpzDropIPRuleMinPrefixLengthIPv4 = uint32Ptr(24)
				return f
			}(),
		},
		{
			name:    "UseRpzDropIPRule/RpzDropIPRuleMinPrefixLengthIPv6",
			desired: func() dnsViewFields { f := base(); f.UseRpzDropIPRule = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseRpzDropIPRule = boolPtr(false)
				f.RpzDropIPRuleMinPrefixLengthIPv6 = uint32Ptr(64)
				return f
			}(),
		},
		{
			name:    "UseRpzQnameWaitRecurse/RpzQnameWaitRecurse",
			desired: func() dnsViewFields { f := base(); f.UseRpzQnameWaitRecurse = boolPtr(false); return f }(),
			observed: func() dnsViewFields {
				f := base()
				f.UseRpzQnameWaitRecurse = boolPtr(false)
				f.RpzQnameWaitRecurse = boolPtr(true)
				return f
			}(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !isUpToDate(tc.desired, tc.observed) {
				t.Errorf("isUpToDate(%s): want true (flag off, value is server-owned), got false (non-convergent drift comparison)", tc.name)
			}
		})
	}
}

// TestIsUpToDateDetectsGatedValueWhenFlagOn is the flag-on counterpart of
// TestIsUpToDateIgnoresGatedValueWhenFlagOff: a representative sample of
// gated pairs still detect a genuine mismatch once the flag is on.
func TestIsUpToDateDetectsGatedValueWhenFlagOn(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), UseLameTTL: boolPtr(true), LameTTL: uint32Ptr(30)}
	observed := dnsViewFields{Name: stringPtr("v"), UseLameTTL: boolPtr(true), LameTTL: uint32Ptr(600)}
	if isUpToDate(desired, observed) {
		t.Error("isUpToDate: want false (UseLameTTL on, LameTTL differs), got true")
	}
}

// TestIsUpToDateDetectsUseFlagTransition proves the flag's own comparison
// stays unconditional: a true -> false transition on the flag itself is
// still reported as drift even though the gate suppresses the paired
// value's comparison in that state.
func TestIsUpToDateDetectsUseFlagTransition(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), UseLameTTL: boolPtr(false)}
	observed := dnsViewFields{Name: stringPtr("v"), UseLameTTL: boolPtr(true), LameTTL: uint32Ptr(600)}
	if isUpToDate(desired, observed) {
		t.Error("isUpToDate: want false (UseLameTTL transitioned true -> false), got true")
	}
}

// TestIsUpToDateGatedPointerStruct covers the *responseRateLimitingValue /
// *scavengingSettingsValue shape: gated on a use flag but compared as a
// whole nested struct pointer via gatedPtrDeepEqual.
func TestIsUpToDateGatedPointerStruct(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), UseResponseRateLimiting: boolPtr(false)}
	observed := dnsViewFields{
		Name:                    stringPtr("v"),
		UseResponseRateLimiting: boolPtr(false),
		ResponseRateLimiting:    &responseRateLimitingValue{ResponsesPerSecond: uint32Ptr(20)},
	}
	if !isUpToDate(desired, observed) {
		t.Error("isUpToDate: want true (UseResponseRateLimiting off, ResponseRateLimiting is server-owned), got false")
	}
}

// TestIsUpToDateGatedNestedSlice covers the CustomRootNameServers /
// DnssecTrustedKeys / FixedRrsetOrderFqdns / FilterAaaaList / Sortlist
// shape: a nested-value-bag slice gated on an outer use flag via
// gatedNestedSliceEqual.
func TestIsUpToDateGatedNestedSlice(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), UseRootNameServer: boolPtr(false)}
	observed := dnsViewFields{
		Name:                  stringPtr("v"),
		UseRootNameServer:     boolPtr(false),
		CustomRootNameServers: []nameServerValue{{Name: strPtrOrNil("ns1.example.com")}},
	}
	if !isUpToDate(desired, observed) {
		t.Error("isUpToDate: want true (UseRootNameServer off, CustomRootNameServers is server-owned), got false")
	}
}

// ── nested use_tsig_key_name gating (dnssec/root-server/filter/match ACL
// entries) — the SDK documents use_tsig_key_name as the use flag for
// tsig_key_name on each ACL/name-server entry individually, not on the
// View object as a whole, so the gate lives inside nameServerValueEqual /
// addressAcValueEqual rather than dnsViewFieldComparators. ───────────────

// TestNameServerValuesEqualIgnoresTsigKeyNameWhenFlagOff proves a
// CustomRootNameServers entry ignores a tsig_key_name mismatch while its
// own use_tsig_key_name is off.
func TestNameServerValuesEqualIgnoresTsigKeyNameWhenFlagOff(t *testing.T) {
	a := []nameServerValue{{Name: strPtrOrNil("ns1.example.com"), UseTsigKeyName: boolPtr(false), TsigKeyName: strPtrOrNil("key-a")}}
	b := []nameServerValue{{Name: strPtrOrNil("ns1.example.com"), UseTsigKeyName: boolPtr(false), TsigKeyName: strPtrOrNil("key-b")}}
	if !nameServerValuesEqual(a, b) {
		t.Error("nameServerValuesEqual: want true (use_tsig_key_name off, tsig_key_name is server-owned), got false")
	}
}

// TestNameServerValuesEqualDetectsTsigKeyNameWhenFlagOn is the flag-on
// counterpart: the same mismatch is real drift once the flag is on.
func TestNameServerValuesEqualDetectsTsigKeyNameWhenFlagOn(t *testing.T) {
	a := []nameServerValue{{Name: strPtrOrNil("ns1.example.com"), UseTsigKeyName: boolPtr(true), TsigKeyName: strPtrOrNil("key-a")}}
	b := []nameServerValue{{Name: strPtrOrNil("ns1.example.com"), UseTsigKeyName: boolPtr(true), TsigKeyName: strPtrOrNil("key-b")}}
	if nameServerValuesEqual(a, b) {
		t.Error("nameServerValuesEqual: want false (use_tsig_key_name on, tsig_key_name differs), got true")
	}
}

// TestNameServerValuesEqualDetectsUseTsigKeyNameTransition proves the
// per-item flag comparison stays unconditional even though the value
// comparison is gated.
func TestNameServerValuesEqualDetectsUseTsigKeyNameTransition(t *testing.T) {
	a := []nameServerValue{{Name: strPtrOrNil("ns1.example.com"), UseTsigKeyName: boolPtr(false)}}
	b := []nameServerValue{{Name: strPtrOrNil("ns1.example.com"), UseTsigKeyName: boolPtr(true), TsigKeyName: strPtrOrNil("key-b")}}
	if nameServerValuesEqual(a, b) {
		t.Error("nameServerValuesEqual: want false (use_tsig_key_name transitioned false -> true), got true")
	}
}

// TestAddressAcValuesEqualIgnoresTsigKeyNameWhenFlagOff proves a
// MatchClients/MatchDestinations/FilterAaaaList entry (which has no outer
// use flag of its own — see the comparator table's doc comment) still
// gates its own tsig_key_name on use_tsig_key_name.
func TestAddressAcValuesEqualIgnoresTsigKeyNameWhenFlagOff(t *testing.T) {
	a := []addressAcValue{{Address: strPtrOrNil("10.0.0.0/24"), UseTsigKeyName: boolPtr(false), TsigKeyName: strPtrOrNil("key-a")}}
	b := []addressAcValue{{Address: strPtrOrNil("10.0.0.0/24"), UseTsigKeyName: boolPtr(false), TsigKeyName: strPtrOrNil("key-b")}}
	if !addressAcValuesEqual(a, b) {
		t.Error("addressAcValuesEqual: want true (use_tsig_key_name off, tsig_key_name is server-owned), got false")
	}
}

// TestAddressAcValuesEqualDetectsTsigKeyNameWhenFlagOn is the flag-on
// counterpart.
func TestAddressAcValuesEqualDetectsTsigKeyNameWhenFlagOn(t *testing.T) {
	a := []addressAcValue{{Address: strPtrOrNil("10.0.0.0/24"), UseTsigKeyName: boolPtr(true), TsigKeyName: strPtrOrNil("key-a")}}
	b := []addressAcValue{{Address: strPtrOrNil("10.0.0.0/24"), UseTsigKeyName: boolPtr(true), TsigKeyName: strPtrOrNil("key-b")}}
	if addressAcValuesEqual(a, b) {
		t.Error("addressAcValuesEqual: want false (use_tsig_key_name on, tsig_key_name differs), got true")
	}
}

// ── lateInitializeFields gating (mirrors the isUpToDate gate) ───────────

// TestLateInitializeGatesStringSliceWhenFlagOff proves a []string field
// gated by a use flag (Forwarders/UseForwarders) is not back-filled while
// the flag is off.
func TestLateInitializeGatesStringSliceWhenFlagOff(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), UseForwarders: boolPtr(false)}
	observed := dnsViewFields{Name: stringPtr("v"), UseForwarders: boolPtr(false), Forwarders: []string{"8.8.8.8"}}

	got, _ := lateInitializeFields(desired, observed)
	if len(got.Forwarders) != 0 {
		t.Errorf("lateInitializeFields: Forwarders = %v, want empty (UseForwarders is off)", got.Forwarders)
	}
}

// TestLateInitializeGatesNestedSliceWhenFlagOff proves a nested-value-bag
// slice gated by an outer use flag (CustomRootNameServers/
// UseRootNameServer) is not back-filled while the flag is off.
func TestLateInitializeGatesNestedSliceWhenFlagOff(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v"), UseRootNameServer: boolPtr(false)}
	observed := dnsViewFields{
		Name:                  stringPtr("v"),
		UseRootNameServer:     boolPtr(false),
		CustomRootNameServers: []nameServerValue{{Name: strPtrOrNil("ns1.example.com")}},
	}

	got, _ := lateInitializeFields(desired, observed)
	if len(got.CustomRootNameServers) != 0 {
		t.Errorf("lateInitializeFields: CustomRootNameServers = %v, want empty (UseRootNameServer is off)", got.CustomRootNameServers)
	}
}

// TestLateInitializeGatesUsingEffectiveFlagFromObserved proves the gate
// resolves the flag's *effective* value — the one observed will back-fill
// to, since desired leaves it nil — rather than depending on which op in
// the table happens to run first. Op ordering in dnsViewLateInitOps places
// several gated values before their own flag's op; if the gate read
// desired.UseX directly (nil at that point) instead of falling through to
// observed.UseX, it would wrongly treat "unset" as "off" even when
// observed will back-fill the flag to true in the very same call.
func TestLateInitializeGatesUsingEffectiveFlagFromObserved(t *testing.T) {
	desired := dnsViewFields{Name: stringPtr("v")} // UseLameTTL unset
	observed := dnsViewFields{
		Name:       stringPtr("v"),
		UseLameTTL: boolPtr(true),
		LameTTL:    uint32Ptr(45),
	}

	got, _ := lateInitializeFields(desired, observed)
	if got.LameTTL == nil || *got.LameTTL != 45 {
		t.Errorf("lateInitializeFields: LameTTL = %v, want 45 (UseLameTTL will back-fill to true from observed)", got.LameTTL)
	}
}

// ── V.C51-class fix: nestedSliceEqual empty/nil equivalence ─────────────

// TestNestedSliceEqualTreatsNilAndEmptyAsEqual proves nestedSliceEqual
// (used for DnssecTrustedKeys, FixedRrsetOrderFqdns, Sortlist, and every
// other nested-value-bag list without its own use-flag pair) does not
// report drift when one side is an explicit nil slice (as would arrive
// from a WAPI response that omits an empty list) and the other is a
// non-nil empty slice built by the CRD's own conversion helpers.
func TestNestedSliceEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	var nilSide []dnssecTrustedKeyValue
	emptySide := []dnssecTrustedKeyValue{}
	if !nestedSliceEqual(nilSide, emptySide) {
		t.Error("nestedSliceEqual: want true for nil vs empty slice, got false (non-convergent drift comparison)")
	}
}

// ── identity ladder: Ambiguous match refusal ────────────────────────────

func TestClusterObserveAmbiguousMatchRefusesAndDoesNotMutate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	uid := "test-uid-cluster"
	ref1 := m.seed(&ibclient.View{Name: stringPtr("view-one"), Ea: ibclient.EA{identity.EAKey: uid}})
	ref2 := m.seed(&ibclient.View{Name: stringPtr("view-two"), Ea: ibclient.EA{identity.EAKey: uid}})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "")
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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	uid := "test-uid-namespaced"
	ref1 := m.seed(&ibclient.View{Name: stringPtr("view-one"), Ea: ibclient.EA{identity.EAKey: uid}})
	ref2 := m.seed(&ibclient.View{Name: stringPtr("view-two"), Ea: ibclient.EA{identity.EAKey: uid}})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", "", "ProviderConfig")
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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name: stringPtr("my-view"),
		// No identity.EAKey stamped — this object is unowned.
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", ref)

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.View{
		Name: stringPtr("my-view"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedDNSView("default", "my-dnsview", ref, "ProviderConfig")

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "")

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "")
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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterDNSView("my-dnsview", "")
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

// ── rotation: persistence round-trips through a client ──────────────────

func TestClusterObserveRecoversRotatedRefPersistsAcrossReGet(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	cr := newClusterDNSView("my-dnsview", "view/stale-ref:my-view/false")
	newRef := m.seed(&ibclient.View{Name: stringPtr("my-view"), Ea: ibclient.EA{identity.EAKey: string(cr.GetUID())}})

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}

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

	fetched := &clusterv1alpha1.DNSView{}
	if err := kube.Get(context.Background(), types.NamespacedName{Name: cr.GetName()}, fetched); err != nil {
		t.Fatalf("kube.Get: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(fetched); got != newRef {
		t.Errorf("Observe: persisted external-name (re-GET into a distinct object) = %q, want %q", got, newRef)
	}
}

// ── newViewForGet: the constructor actually passed to Resolve ───────────
//
// internal/clients/identity's own TestNewEmptyCorrectness exercises
// ibclient.NewEmptyDNSView() directly, documenting the SDK baseline —
// but resolveViewIdentity passes this package's own newViewForGet, not
// that raw constructor, to identity.Resolve. This test closes that gap.
func TestNewViewForGetObjectTypeAndReturnFields(t *testing.T) {
	v := newViewForGet()
	if got := v.ObjectType(); got != "view" {
		t.Errorf("newViewForGet().ObjectType() = %q, want %q", got, "view")
	}
	fields := v.ReturnFields()
	found := false
	for _, f := range fields {
		if f == "extattrs" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("newViewForGet().ReturnFields() = %v, want it to contain %q — identity.Resolve reads the identity stamp from the Ea field, which this field populates on GET", fields, "extattrs")
	}
}
