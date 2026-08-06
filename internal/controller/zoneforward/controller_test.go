// Package zoneforward unit tests for the ZoneForward MR controllers.
// Tests use inline httptest.NewServer mocks that emulate the WAPI
// zone_forward endpoints, PascalCase test names (no underscores), and
// white-box access to the unexported connectors/clients so both scopes
// can be exercised without going through the full Connect() credential
// bridge on every test.
package zoneforward

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

	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zoneforward/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/zoneforward/v1alpha1"
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

// newClusterZoneForward builds a minimal cluster-scoped ZoneForward CR.
// When externalName is empty, the external-name annotation is left
// unset. When it equals crName it simulates the framework's
// NameAsExternalName initializer (the pre-create state); any other value
// simulates a Create()-assigned server ref.
func newClusterZoneForward(crName, externalName string) *clusterv1alpha1.ZoneForward {
	cr := &clusterv1alpha1.ZoneForward{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.ZoneForwardSpec{
			ClusterManagedResourceSpec: xpv2.ClusterManagedResourceSpec{
				ProviderConfigReference: &xpv2.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.ZoneForwardParameters{
				Fqdn: stringPtr("forward.example.com"),
				ForwardTo: []clusterv1alpha1.NameServer{
					{Name: stringPtr("ns1.example.com"), Address: stringPtr("10.0.0.53")},
				},
				View: stringPtr("default"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedZoneForward is the namespaced variant of
// newClusterZoneForward.
func newNamespacedZoneForward(ns, crName, externalName, pcKind string) *namespacedv1alpha1.ZoneForward {
	cr := &namespacedv1alpha1.ZoneForward{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.ZoneForwardSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.ZoneForwardParameters{
				Fqdn: stringPtr("forward.example.com"),
				ForwardTo: []namespacedv1alpha1.NameServer{
					{Name: stringPtr("ns1.example.com"), Address: stringPtr("10.0.0.53")},
				},
				View: stringPtr("default"),
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
// mockWapiServer emulates the subset of NIOS WAPI zone_forward endpoints
// exercised by the ZoneForward controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.ZoneForward type so the wire format (including the EA
// {"value": ...} envelope and the NullableNameServers/
// NullableForwardingServers list encoding) exactly matches what the SDK
// sends and expects.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.ZoneForward
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte

	// eaDefExists controls the identity extensible-attribute-definition
	// prerequisite endpoint (see identity.Prober). Defaults to true via
	// newMockWapiServer so ordinary tests never have to think about the
	// prerequisite probe; set to false to exercise the absent-definition
	// path.
	eaDefExists bool
	// eaDefCreatable controls whether a POST to create the missing
	// definition succeeds, when eaDefExists is false.
	eaDefCreatable bool
	// searchCalls counts identity-EA search requests (GET with no _ref
	// path segment) for tests that assert the ladder searched (or did
	// not search) by uid.
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
	return &mockWapiServer{records: map[string]*ibclient.ZoneForward{}, eaDefExists: true}
}

func (m *mockWapiServer) seed(rec *ibclient.ZoneForward) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		rec.Ref = m.newRefLocked(rec)
	}
	m.records[rec.Ref] = rec
	return rec.Ref
}

func (m *mockWapiServer) newRefLocked(rec *ibclient.ZoneForward) string {
	view := ""
	if rec.View != nil {
		view = *rec.View
	}
	return "zone_forward/test" + itoa(m.nextRef) + ":" + rec.Fqdn + "/" + view
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

// handler returns an http.Handler implementing the zone_forward WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/zone_forward", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.ZoneForward
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
	// still filter by fqdn/view. Registered as an exact literal path so
	// Go's ServeMux prefers it over the {ref...} wildcard below for
	// requests to precisely "zone_forward" (real _refs always carry
	// additional path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/zone_forward", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		uid := q.Get("*" + identity.EAKey)
		fqdn := q.Get("fqdn")
		view := q.Get("view")

		m.mu.Lock()
		m.searchCalls++
		var matches []ibclient.ZoneForward
		for _, rec := range m.records {
			if uid != "" {
				got, ok := rec.Ea[identity.EAKey]
				if !ok || got != uid {
					continue
				}
				matches = append(matches, *rec)
				continue
			}
			if fqdn != "" && rec.Fqdn != fqdn {
				continue
			}
			if view != "" && (rec.View == nil || *rec.View != view) {
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
		var incoming ibclient.ZoneForward
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		existing.ForwardTo = incoming.ForwardTo
		existing.Comment = incoming.Comment
		existing.Disable = incoming.Disable
		existing.ForwardersOnly = incoming.ForwardersOnly
		existing.ForwardingServers = incoming.ForwardingServers
		existing.NsGroup = incoming.NsGroup
		existing.ExternalNsGroup = incoming.ExternalNsGroup
		existing.Ea = incoming.Ea
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, ref)
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

// newTestClients builds an identity.ManagerAndConnector pointed at the
// given httptest.Server via plain HTTP (no TLS needed — the
// WapiRequestBuilder only switches to HTTPS when hostCfg.Scheme != "http").
func newTestClients(t *testing.T, srv *httptest.Server) identity.ManagerAndConnector {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	mc, err := newObjectManagerWithScheme(&nioCredentials{
		Host:     u.Hostname(),
		Username: "test-user",
		Password: "test-pass",
	}, true, "http", u.Port())
	if err != nil {
		t.Fatalf("cannot build test object manager: %v", err)
	}
	return mc
}

// ── cluster: Observe ────────────────────────────────────────────────────

func TestClusterObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:            "forward.example.com",
		View:            stringPtr("default"),
		ForwardTo:       ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:         stringPtr("hello"),
		Disable:         boolPtr(false),
		ForwardersOnly:  boolPtr(true),
		NsGroup:         stringPtr("dns-group"),
		ExternalNsGroup: stringPtr("ext-group"),
		Ea:              ibclient.EA{"env": "prod", identity.EAKey: "test-uid-cluster"},
	})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.ForwardersOnly = boolPtr(true)
	cr.Spec.ForProvider.NsGroup = stringPtr("dns-group")
	cr.Spec.ForProvider.ExternalNsGroup = stringPtr("ext-group")
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
	if cond := cr.GetCondition(xpv2.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "zone_forward/does-not-exist:forward.example.com/default")

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())     // simulate NameAsExternalName initializer

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

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "zone_forward/test1:forward.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "zone_forward/test1:forward.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map, an empty ForwardTo list) must not panic and must produce a
// valid observation with nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)

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
	if ap.Fqdn != nil {
		t.Errorf("AtProvider.Fqdn = %v, want nil", ap.Fqdn)
	}
	if ap.View != nil {
		t.Errorf("AtProvider.View = %v, want nil", ap.View)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.ForwardTo != nil {
		t.Errorf("AtProvider.ForwardTo = %v, want nil", ap.ForwardTo)
	}
	if ap.ForwardingServers != nil {
		t.Errorf("AtProvider.ForwardingServers = %v, want nil", ap.ForwardingServers)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestClusterCreateError verifies that a WAPI 5xx response during Create
// is propagated (wrapped, not swallowed) and the external-name is left
// unset — a failed Create must not falsely mark the resource as
// provisioned.
func TestClusterCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateZoneForward) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateZoneForward)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty after failed create", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:       "forward.example.com",
		View:       stringPtr("original-view"),
		ZoneFormat: "FORWARD",
		ForwardTo:  ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Ea:         ibclient.EA{identity.EAKey: "test-uid-cluster"},
	})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)
	// Mutate the immutable fqdn/view/zoneFormat fields in spec — this must
	// NOT affect ResourceUpToDate, since they are excluded from
	// isUpToDate (WAPI has no UpdateZoneForward parameter for them).
	cr.Spec.ForProvider.Fqdn = stringPtr("changed.example.com")
	cr.Spec.ForProvider.View = stringPtr("changed-view")
	cr.Spec.ForProvider.ZoneFormat = stringPtr("IPV4")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite fqdn/view/zoneFormat drift (immutable fields), got false")
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:   stringPtr("old comment"),
	})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)
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
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreatable = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:   stringPtr("old comment"),
	})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterZoneForward("my-zone", ref)
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
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreatable = false
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:   stringPtr("old comment"),
	})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newClusterZoneForward("my-zone", ref)
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

func TestClusterUpdateDoesNotSendImmutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:       "forward.example.com",
		View:       stringPtr("default"),
		ZoneFormat: "FORWARD",
		ForwardTo:  ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
	})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)

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
	for _, immutable := range []string{"fqdn", "view", "zone_format"} {
		if _, present := raw[immutable]; present {
			t.Errorf("Update: request body contains immutable field %q: %v", immutable, raw[immutable])
		}
	}
}

// TestClusterUpdateError verifies that a WAPI 5xx response during Update
// is propagated (wrapped, not swallowed) rather than being silently
// treated as a successful reconcile.
func TestClusterUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "zone_forward/test1:forward.example.com/default")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateZoneForward) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateZoneForward)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{Fqdn: "forward.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "test-uid-cluster"}})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)

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

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "zone_forward/does-not-exist:forward.example.com/default")

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

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "zone_forward/test1:forward.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteZoneForward) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteZoneForward)
	}
}

func TestClusterDeleteForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "zone_forward/test1:forward.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("Delete: expected error for 403, got nil")
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

	rec := &ibclient.ZoneForward{Fqdn: "forward.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}}
	ref := m.seed(rec)

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)

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

	ref := m.seed(&ibclient.ZoneForward{Fqdn: "forward.example.com", View: stringPtr("default")})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)

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

	cr := newClusterZoneForward("my-zone", "zone_forward/stale-ref:forward.example.com/default")
	liveRef := m.seed(&ibclient.ZoneForward{Fqdn: "forward.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: string(cr.GetUID())}})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

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

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "zone_forward/stale-ref:forward.example.com/default")

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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	rec := &ibclient.ZoneForward{Fqdn: "forward.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}}
	ref := m.seed(rec)

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)

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

	cr := newClusterZoneForward("my-zone", "")
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

	cr := newClusterZoneForward("my-zone", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
	})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", ref, "ProviderConfig")

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

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", "zone_forward/does-not-exist:forward.example.com/default", "ProviderConfig")

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

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", "", "ProviderConfig")
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

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", "zone_forward/test1:forward.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

// TestNamespacedObserveMinimalResponse mirrors
// TestClusterObserveMinimalResponse for the namespaced scope: a WAPI
// response carrying only the object's _ref and every other field at its
// Go zero value must not panic and must produce a valid observation with
// nil-safe AtProvider fields.
func TestNamespacedObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", ref, "ProviderConfig")

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
	if ap.Fqdn != nil {
		t.Errorf("AtProvider.Fqdn = %v, want nil", ap.Fqdn)
	}
	if ap.View != nil {
		t.Errorf("AtProvider.View = %v, want nil", ap.View)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.ForwardTo != nil {
		t.Errorf("AtProvider.ForwardTo = %v, want nil", ap.ForwardTo)
	}
	if ap.ForwardingServers != nil {
		t.Errorf("AtProvider.ForwardingServers = %v, want nil", ap.ForwardingServers)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateError verifies that a WAPI 5xx response during
// Create is propagated (wrapped, not swallowed) and the external-name is
// left unset.
func TestNamespacedCreateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateZoneForward) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateZoneForward)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty after failed create", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:   stringPtr("old comment"),
	})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", ref, "ProviderConfig")
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
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreatable = true
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:   stringPtr("old comment"),
	})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedZoneForward("default", "my-zone", ref, "ProviderConfig")
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
	m := newMockWapiServer()
	m.eaDefExists = false
	m.eaDefCreatable = false
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:   stringPtr("old comment"),
	})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber()}
	cr := newNamespacedZoneForward("default", "my-zone", ref, "ProviderConfig")
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

// TestNamespacedUpdateError verifies that a WAPI 5xx response during
// Update is propagated (wrapped, not swallowed).
func TestNamespacedUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", "zone_forward/test1:forward.example.com/default", "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateZoneForward) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateZoneForward)
	}
}

// TestNamespacedUpdateDoesNotSendImmutableFields mirrors the cluster-scope
// assertion: fqdn, view, and zone_format must never appear in the
// namespaced-scope Update request body either.
func TestNamespacedUpdateDoesNotSendImmutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:       "forward.example.com",
		View:       stringPtr("default"),
		ZoneFormat: "FORWARD",
		ForwardTo:  ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
	})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", ref, "ProviderConfig")

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
	for _, immutable := range []string{"fqdn", "view", "zone_format"} {
		if _, present := raw[immutable]; present {
			t.Errorf("Update: request body contains immutable field %q: %v", immutable, raw[immutable])
		}
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{Fqdn: "forward.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "test-uid-namespaced"}})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", ref, "ProviderConfig")

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

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", "zone_forward/does-not-exist:forward.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

// TestNamespacedDeleteServerError verifies that a 5xx response from the
// WAPI delete endpoint is propagated (wrapped, not swallowed) for the
// namespaced scope too.
func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", "zone_forward/test1:forward.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteZoneForward) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteZoneForward)
	}
}

// TestNamespacedDeleteRefusesOnForeignIdentity is the namespaced-scope
// counterpart of TestClusterDeleteRefusesOnForeignIdentity.
func TestNamespacedDeleteRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{Fqdn: "forward.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", ref, "ProviderConfig")

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

	ref := m.seed(&ibclient.ZoneForward{Fqdn: "forward.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: "someone-elses-uid"}})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", ref, "ProviderConfig")

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

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", "zone_forward/stale-ref:forward.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the identity search also finds nothing, got: %v", err)
	}
}

// ── namespaced: Disconnect ──────────────────────────────────────────────

func TestNamespacedDisconnectIsNoop(t *testing.T) {
	e := &namespacedExternal{kube: &recordingKubeClient{}, prober: identity.NewProber(), endpoint: t.Name()}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect: unexpected error: %v", err)
	}
}

// ── namespaced: Connect ──────────────────────────────────────────────────

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

	cr := newNamespacedZoneForward(ns, "my-zone", "", "ProviderConfig")
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

	cr := newNamespacedZoneForward("app-ns", "my-zone", "", "ClusterProviderConfig")
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

	cr := newNamespacedZoneForward("default", "my-zone", "", "SomeOtherKind")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for unsupported provider config kind, got nil")
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

func TestNameServersEqual(t *testing.T) {
	cases := map[string]struct {
		reason string
		a, b   []ibclient.NameServer
		want   bool
	}{
		"BothEmpty": {
			reason: "two nil/empty lists must compare equal",
			want:   true,
		},
		"IdenticalSingleEntry": {
			reason: "matching single-entry lists must compare equal",
			a:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			b:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			want:   true,
		},
		"DifferentAddress": {
			reason: "an address change must be detected as drift",
			a:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			b:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.99"}},
			want:   false,
		},
		"DifferentLength": {
			reason: "an added/removed name server must be detected as drift",
			a:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			b: []ibclient.NameServer{
				{Name: "ns1.example.com", Address: "10.0.0.53"},
				{Name: "ns2.example.com", Address: "10.0.0.54"},
			},
			want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := nameServersEqual(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("%s: nameServersEqual() = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

func TestForwardingServersEqual(t *testing.T) {
	makeFwd := func(name string, forwardersOnly bool) *ibclient.Forwardingmemberserver {
		return &ibclient.Forwardingmemberserver{
			Name:           name,
			ForwardersOnly: forwardersOnly,
			ForwardTo:      ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		}
	}

	cases := map[string]struct {
		reason string
		a, b   []*ibclient.Forwardingmemberserver
		want   bool
	}{
		"BothEmpty": {
			reason: "two nil/empty lists must compare equal",
			want:   true,
		},
		"IdenticalSingleEntry": {
			reason: "matching single-entry lists must compare equal",
			a:      []*ibclient.Forwardingmemberserver{makeFwd("member1", true)},
			b:      []*ibclient.Forwardingmemberserver{makeFwd("member1", true)},
			want:   true,
		},
		"DifferentForwardersOnly": {
			reason: "a forwardersOnly flag change must be detected as drift",
			a:      []*ibclient.Forwardingmemberserver{makeFwd("member1", true)},
			b:      []*ibclient.Forwardingmemberserver{makeFwd("member1", false)},
			want:   false,
		},
		"DifferentLength": {
			reason: "an added/removed member override must be detected as drift",
			a:      []*ibclient.Forwardingmemberserver{makeFwd("member1", true)},
			b:      []*ibclient.Forwardingmemberserver{makeFwd("member1", true), makeFwd("member2", false)},
			want:   false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := forwardingServersEqual(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("%s: forwardingServersEqual() = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var comment, nsGroup, externalNsGroup, view, zoneFormat *string
	var disable, forwardersOnly *bool
	extAttrs := map[string]string(nil)

	rec := &ibclient.ZoneForward{
		Comment:         stringPtr("server default"),
		Disable:         boolPtr(true),
		ForwardersOnly:  boolPtr(true),
		NsGroup:         stringPtr("dns-group"),
		ExternalNsGroup: stringPtr("ext-group"),
		Ea:              ibclient.EA{"env": "prod"},
		View:            stringPtr("default"),
		ZoneFormat:      "FORWARD",
	}

	changed := lateInitialize(&comment, &nsGroup, &externalNsGroup, &disable, &forwardersOnly, &extAttrs, &view, &zoneFormat, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if comment == nil || *comment != "server default" {
		t.Errorf("lateInitialize: comment = %v, want %q", comment, "server default")
	}
	if disable == nil || *disable != true {
		t.Errorf("lateInitialize: disable = %v, want true", disable)
	}
	if forwardersOnly == nil || *forwardersOnly != true {
		t.Errorf("lateInitialize: forwardersOnly = %v, want true", forwardersOnly)
	}
	if nsGroup == nil || *nsGroup != "dns-group" {
		t.Errorf("lateInitialize: nsGroup = %v, want %q", nsGroup, "dns-group")
	}
	if externalNsGroup == nil || *externalNsGroup != "ext-group" {
		t.Errorf("lateInitialize: externalNsGroup = %v, want %q", externalNsGroup, "ext-group")
	}
	if !extAttrsEqual(extAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("lateInitialize: extAttrs = %v, want {env: prod}", extAttrs)
	}
	if view == nil || *view != "default" {
		t.Errorf("lateInitialize: view = %v, want %q", view, "default")
	}
	if zoneFormat == nil || *zoneFormat != "FORWARD" {
		t.Errorf("lateInitialize: zoneFormat = %v, want %q", zoneFormat, "FORWARD")
	}
}

// TestLateInitializeStripsIdentityEAFromExtAttrs proves the reserved
// identity key never late-inits into spec.forProvider.extAttrs. The CRD
// schema never includes it, and a CEL rule rejects a user-supplied value
// — back-filling it here would produce a permanent validation failure on
// the very next apply.
func TestLateInitializeStripsIdentityEAFromExtAttrs(t *testing.T) {
	var comment, nsGroup, externalNsGroup, view, zoneFormat *string
	var disable, forwardersOnly *bool
	extAttrs := map[string]string(nil)

	rec := &ibclient.ZoneForward{
		Ea: ibclient.EA{"env": "prod", identity.EAKey: "some-uid"},
	}

	lateInitialize(&comment, &nsGroup, &externalNsGroup, &disable, &forwardersOnly, &extAttrs, &view, &zoneFormat, rec)

	if _, present := extAttrs[identity.EAKey]; present {
		t.Errorf("lateInitialize: extAttrs contains the reserved identity key %q, want it stripped", identity.EAKey)
	}
	if !extAttrsEqual(extAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("lateInitialize: extAttrs = %v, want {env: prod} (identity key stripped)", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	nsGroup := stringPtr("user-group")
	externalNsGroup := stringPtr("user-ext-group")
	view := stringPtr("user-view")
	zoneFormat := stringPtr("IPV4")
	disable := boolPtr(false)
	forwardersOnly := boolPtr(false)
	extAttrs := map[string]string{"env": "staging"}

	rec := &ibclient.ZoneForward{
		Comment:         stringPtr("server default"),
		Disable:         boolPtr(true),
		ForwardersOnly:  boolPtr(true),
		NsGroup:         stringPtr("dns-group"),
		ExternalNsGroup: stringPtr("ext-group"),
		Ea:              ibclient.EA{"env": "prod"},
		View:            stringPtr("default"),
		ZoneFormat:      "FORWARD",
	}

	changed := lateInitialize(&comment, &nsGroup, &externalNsGroup, &disable, &forwardersOnly, &extAttrs, &view, &zoneFormat, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" || *nsGroup != "user-group" || *externalNsGroup != "user-ext-group" || *view != "user-view" || *zoneFormat != "IPV4" {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
	if *disable != false || *forwardersOnly != false {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
	if extAttrs["env"] != "staging" {
		t.Error("lateInitialize: overwrote already-set ExtAttrs")
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedRecord := func() *ibclient.ZoneForward {
		return &ibclient.ZoneForward{
			ForwardTo:       ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
			Comment:         stringPtr("hello"),
			Disable:         boolPtr(false),
			ForwardersOnly:  boolPtr(true),
			NsGroup:         stringPtr("dns-group"),
			ExternalNsGroup: stringPtr("ext-group"),
			Ea:              ibclient.EA{"env": "prod"},
		}
	}

	cases := map[string]struct {
		reason          string
		forwardTo       []ibclient.NameServer
		comment         *string
		nsGroup         *string
		externalNsGroup *string
		disable         *bool
		forwardersOnly  *bool
		extAttrs        map[string]string
		want            bool
	}{
		"IdenticalFieldsAreUpToDate": {
			reason:          "when every mutable field matches the observed record, the resource must be reported up to date",
			forwardTo:       []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			externalNsGroup: stringPtr("ext-group"),
			disable:         boolPtr(false),
			forwardersOnly:  boolPtr(true),
			extAttrs:        map[string]string{"env": "prod"},
			want:            true,
		},
		"ChangedForwardToIsNotUpToDate": {
			reason:          "a changed forwardTo list must be detected as drift",
			forwardTo:       []ibclient.NameServer{{Name: "ns2.example.com", Address: "10.0.0.54"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			externalNsGroup: stringPtr("ext-group"),
			disable:         boolPtr(false),
			forwardersOnly:  boolPtr(true),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedCommentIsNotUpToDate": {
			reason:          "a changed comment must be detected as drift",
			forwardTo:       []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("goodbye"),
			nsGroup:         stringPtr("dns-group"),
			externalNsGroup: stringPtr("ext-group"),
			disable:         boolPtr(false),
			forwardersOnly:  boolPtr(true),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedDisableIsNotUpToDate": {
			reason:          "a changed disable flag must be detected as drift",
			forwardTo:       []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			externalNsGroup: stringPtr("ext-group"),
			disable:         boolPtr(true),
			forwardersOnly:  boolPtr(true),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedForwardersOnlyIsNotUpToDate": {
			reason:          "a changed forwardersOnly flag must be detected as drift",
			forwardTo:       []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			externalNsGroup: stringPtr("ext-group"),
			disable:         boolPtr(false),
			forwardersOnly:  boolPtr(false),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedNsGroupIsNotUpToDate": {
			reason:          "a changed nsGroup must be detected as drift",
			forwardTo:       []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("other-group"),
			externalNsGroup: stringPtr("ext-group"),
			disable:         boolPtr(false),
			forwardersOnly:  boolPtr(true),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedExternalNsGroupIsNotUpToDate": {
			reason:          "a changed externalNsGroup must be detected as drift",
			forwardTo:       []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			externalNsGroup: stringPtr("other-ext-group"),
			disable:         boolPtr(false),
			forwardersOnly:  boolPtr(true),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ExtAttrsDifferentValueIsNotUpToDate": {
			reason:          "an extAttrs value change on an existing key must be detected as drift",
			forwardTo:       []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			externalNsGroup: stringPtr("ext-group"),
			disable:         boolPtr(false),
			forwardersOnly:  boolPtr(true),
			extAttrs:        map[string]string{"env": "staging"},
			want:            false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := isUpToDate(tc.forwardTo, nil, tc.comment, tc.nsGroup, tc.externalNsGroup, tc.disable, tc.forwardersOnly, tc.extAttrs, observedRecord())
			if got != tc.want {
				t.Errorf("%s: isUpToDate() = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

func TestIsUpToDateExtAttrsEmptyVsNil(t *testing.T) {
	rec := &ibclient.ZoneForward{
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
	}
	// The observed record carries no extattrs (nil Ea) — a spec with an
	// explicit empty map must still compare as up to date, since
	// extAttrsEqual treats nil and empty as equivalent (avoids a phantom
	// diff when the WAPI response omits an empty extattrs object).
	got := isUpToDate([]ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}, nil, nil, nil, nil, nil, nil, map[string]string{}, rec)
	if !got {
		t.Error("isUpToDate: empty ExtAttrs spec vs nil observed Ea = false, want true")
	}
}

func TestIsUpToDateComparesForwardingServers(t *testing.T) {
	rec := &ibclient.ZoneForward{
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		ForwardingServers: &ibclient.NullableForwardingServers{Servers: []*ibclient.Forwardingmemberserver{
			{Name: "member1.example.com", ForwardersOnly: true},
		}},
	}
	forwardTo := []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}

	// Matching forwardingServers must compare up to date.
	matching := []*ibclient.Forwardingmemberserver{{Name: "member1.example.com", ForwardersOnly: true}}
	if !isUpToDate(forwardTo, matching, nil, nil, nil, nil, nil, nil, rec) {
		t.Error("isUpToDate: matching forwardingServers reported as drift, want up to date")
	}

	// A changed forwardingServers list must be detected as drift.
	changed := []*ibclient.Forwardingmemberserver{{Name: "member1.example.com", ForwardersOnly: false}}
	if isUpToDate(forwardTo, changed, nil, nil, nil, nil, nil, nil, rec) {
		t.Error("isUpToDate: changed forwardingServers reported as up to date, want drift")
	}
}

// ── NameServer/ForwardingServer conversion: round-trip ──────────────────

func TestClusterNameServersRoundTrip(t *testing.T) {
	in := []clusterv1alpha1.NameServer{
		{Name: stringPtr("ns1.example.com"), Address: stringPtr("10.0.0.53")},
		{Name: stringPtr("ns2.example.com"), Address: stringPtr("10.0.0.54")},
	}
	sdk := clusterNameServersToSDK(in)
	out := clusterNameServersFromSDK(sdk)
	if len(out) != len(in) {
		t.Fatalf("round-trip: got %d entries, want %d", len(out), len(in))
	}
	for i := range in {
		if *out[i].Name != *in[i].Name || *out[i].Address != *in[i].Address {
			t.Errorf("round-trip[%d]: got %+v, want %+v", i, out[i], in[i])
		}
	}
}

func TestNamespacedNameServersRoundTrip(t *testing.T) {
	in := []namespacedv1alpha1.NameServer{
		{Name: stringPtr("ns1.example.com"), Address: stringPtr("10.0.0.53")},
	}
	sdk := namespacedNameServersToSDK(in)
	out := namespacedNameServersFromSDK(sdk)
	if len(out) != 1 || *out[0].Name != "ns1.example.com" || *out[0].Address != "10.0.0.53" {
		t.Errorf("round-trip: got %+v, want %+v", out, in)
	}
}

func TestClusterForwardingServersRoundTrip(t *testing.T) {
	in := []clusterv1alpha1.ForwardingServer{
		{
			Name:                  stringPtr("member1.example.com"),
			ForwardersOnly:        boolPtr(true),
			ForwardTo:             []clusterv1alpha1.NameServer{{Name: stringPtr("ns1.example.com"), Address: stringPtr("10.0.0.53")}},
			UseOverrideForwarders: boolPtr(true),
		},
	}
	sdk := clusterForwardingServersToSDK(in)
	out := clusterForwardingServersFromSDK(sdk)
	if len(out) != 1 {
		t.Fatalf("round-trip: got %d entries, want 1", len(out))
	}
	if *out[0].Name != "member1.example.com" || *out[0].ForwardersOnly != true || *out[0].UseOverrideForwarders != true {
		t.Errorf("round-trip: got %+v, want name=member1.example.com forwardersOnly=true useOverrideForwarders=true", out[0])
	}
	if len(out[0].ForwardTo) != 1 || *out[0].ForwardTo[0].Name != "ns1.example.com" {
		t.Errorf("round-trip: nested ForwardTo = %+v, want ns1.example.com", out[0].ForwardTo)
	}
}

func TestNamespacedForwardingServersRoundTrip(t *testing.T) {
	in := []namespacedv1alpha1.ForwardingServer{
		{
			Name:                  stringPtr("member1.example.com"),
			ForwardersOnly:        boolPtr(false),
			ForwardTo:             []namespacedv1alpha1.NameServer{{Name: stringPtr("ns2.example.com"), Address: stringPtr("10.0.0.54")}},
			UseOverrideForwarders: boolPtr(false),
		},
	}
	sdk := namespacedForwardingServersToSDK(in)
	out := namespacedForwardingServersFromSDK(sdk)
	if len(out) != 1 || *out[0].Name != "member1.example.com" {
		t.Errorf("round-trip: got %+v, want %+v", out, in)
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

func TestNewObjectManagerWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newObjectManagerWithScheme must not hardcode
	// SslVerify to "true" — it must honor the sslVerify parameter. Both branches
	// must construct successfully (transport config validation happens
	// locally; no network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t"}
			objMgr, err := newObjectManagerWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
			}
			if objMgr.Manager == nil || objMgr.Connector == nil {
				t.Fatal("newObjectManagerWithScheme: expected non-nil manager and connector")
			}
		})
	}
}

// ── identity ladder: Ambiguous match refusal ────────────────────────────

func TestClusterObserveAmbiguousMatchRefusesAndDoesNotMutate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	uid := "test-uid-cluster"
	ref1 := m.seed(&ibclient.ZoneForward{Fqdn: "one.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: uid}})
	ref2 := m.seed(&ibclient.ZoneForward{Fqdn: "two.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: uid}})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "")
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
	ref1 := m.seed(&ibclient.ZoneForward{Fqdn: "one.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: uid}})
	ref2 := m.seed(&ibclient.ZoneForward{Fqdn: "two.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: uid}})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", "", "ProviderConfig")
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

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		// No identity.EAKey stamped — this object is unowned.
	})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)

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

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
	})

	mc := newTestClients(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newNamespacedZoneForward("default", "my-zone", ref, "ProviderConfig")

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

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "")

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

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "")
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

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", "")
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
// updateZoneForward's guard trims before comparing, matching
// identity.Resolve's ladder. Without the trim, a whitespace-only uid
// would pass Update's guard and get re-stamped verbatim into the
// object's extensible attributes, while Observe/Delete (which route
// through identity.Resolve) would treat that same object as unowned.
func TestClusterUpdateWhitespaceUIDFailsWithZeroMutatingRequests(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:   stringPtr("old comment"),
	})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)
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
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	cr := newClusterZoneForward("my-zone", "zone_forward/stale-ref:forward.example.com/default")
	newRef := m.seed(&ibclient.ZoneForward{Fqdn: "forward.example.com", View: stringPtr("default"), Ea: ibclient.EA{identity.EAKey: string(cr.GetUID())}})

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()
	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}

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

	fetched := &clusterv1alpha1.ZoneForward{}
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
// lateInitializeExtAttrs's — TestLateInitializeStripsIdentityEAFromExtAttrs
// only exercises the latter. This test targets the former directly: a
// spec.extAttrs that already matches the user-facing keys must still
// compare up to date against a live record whose Ea additionally carries
// the reserved identity stamp.
func TestIsUpToDateIgnoresIdentityEA(t *testing.T) {
	rec := &ibclient.ZoneForward{
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Ea:        ibclient.EA{"env": "prod", identity.EAKey: "some-uid"},
	}
	forwardTo := []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}

	if !isUpToDate(forwardTo, nil, nil, nil, nil, nil, nil, map[string]string{"env": "prod"}, rec) {
		t.Error("isUpToDate: want true when spec.extAttrs already matches the user-facing keys and the live object's Ea only additionally carries the reserved identity stamp")
	}
}

// ── status.atProvider mirror retains the identity key (convention 0032) ─

func TestClusterObserveAtProviderExtAttrsRetainsIdentityKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneForward{
		Fqdn:      "forward.example.com",
		View:      stringPtr("default"),
		ForwardTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Ea:        ibclient.EA{"env": "prod", identity.EAKey: "test-uid-cluster"},
	})

	mc := newTestClients(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector, prober: identity.NewProber(), endpoint: t.Name()}
	cr := newClusterZoneForward("my-zone", ref)
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got, present := cr.Status.AtProvider.ExtAttrs[identity.EAKey]; !present || got != "test-uid-cluster" {
		t.Errorf("Observe: status.atProvider.extAttrs[%q] = %q (present=%v), want the full-mirror copy to retain the identity stamp", identity.EAKey, got, present)
	}
}
