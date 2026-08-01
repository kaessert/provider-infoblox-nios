// Package zonedelegated unit tests for the ZoneDelegated MR controllers.
// Tests use inline httptest.NewServer mocks that emulate the WAPI
// zone_delegated endpoints, PascalCase test names (no underscores), and
// white-box access to the unexported connectors/clients so both scopes
// can be exercised without going through the full Connect() credential
// bridge on every test.
package zonedelegated

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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zonedelegated/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/zonedelegated/v1alpha1"
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

// newClusterZoneDelegated builds a minimal cluster-scoped ZoneDelegated
// CR. When externalName is empty, the external-name annotation is left
// unset. When it equals crName it simulates the framework's
// NameAsExternalName initializer (the pre-create state); any other value
// simulates a Create()-assigned server ref.
func newClusterZoneDelegated(crName, externalName string) *clusterv1alpha1.ZoneDelegated {
	cr := &clusterv1alpha1.ZoneDelegated{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.ZoneDelegatedSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.ZoneDelegatedParameters{
				Fqdn: stringPtr("delegated.example.com"),
				DelegateTo: []clusterv1alpha1.ZoneDelegatedNameServer{
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

// newNamespacedZoneDelegated is the namespaced variant of
// newClusterZoneDelegated.
func newNamespacedZoneDelegated(ns, crName, externalName, pcKind string) *namespacedv1alpha1.ZoneDelegated {
	cr := &namespacedv1alpha1.ZoneDelegated{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.ZoneDelegatedSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.ZoneDelegatedParameters{
				Fqdn: stringPtr("delegated.example.com"),
				DelegateTo: []namespacedv1alpha1.ZoneDelegatedNameServer{
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
// mockWapiServer emulates the subset of NIOS WAPI zone_delegated
// endpoints exercised by the ZoneDelegated controller (POST create,
// GET/PUT/DELETE by _ref). Records are marshaled/unmarshaled using the
// real ibclient.ZoneDelegated type so the wire format (including the EA
// {"value": ...} envelope and the NullableNameServers list encoding)
// exactly matches what the SDK sends and expects.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.ZoneDelegated
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.ZoneDelegated{}}
}

func (m *mockWapiServer) seed(rec *ibclient.ZoneDelegated) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		rec.Ref = m.newRefLocked(rec)
	}
	m.records[rec.Ref] = rec
	return rec.Ref
}

func (m *mockWapiServer) newRefLocked(rec *ibclient.ZoneDelegated) string {
	view := ""
	if rec.View != nil {
		view = *rec.View
	}
	return "zone_delegated/test" + itoa(m.nextRef) + ":" + rec.Fqdn + "/" + view
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

// handler returns an http.Handler implementing the zone_delegated WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/zone_delegated", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.ZoneDelegated
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ref := m.seed(&rec)
		writeJSON(w, http.StatusOK, ref)
	})

	// Search endpoint (zoneDelegatedExistsByNaturalKey, via the SDK's
	// GetZoneDelegated): a GET with no _ref path segment, filtered by
	// the fqdn query param. Registered as an exact literal path so Go's
	// ServeMux prefers it over the {ref...} wildcard below for requests
	// to precisely "zone_delegated" (real _refs always carry additional
	// path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/zone_delegated", func(w http.ResponseWriter, r *http.Request) {
		fqdn := r.URL.Query().Get("fqdn")

		m.mu.Lock()
		var matches []ibclient.ZoneDelegated
		for _, rec := range m.records {
			if fqdn != "" && rec.Fqdn != fqdn {
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
		var incoming ibclient.ZoneDelegated
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		existing.DelegateTo = incoming.DelegateTo
		existing.Comment = incoming.Comment
		existing.Disable = incoming.Disable
		existing.Locked = incoming.Locked
		existing.NsGroup = incoming.NsGroup
		existing.DelegatedTtl = incoming.DelegatedTtl
		existing.UseDelegatedTtl = incoming.UseDelegatedTtl
		existing.Ea = incoming.Ea
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, ref)
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

// newTestObjectManager builds an ibclient.IBObjectManager pointed at the
// given httptest.Server via plain HTTP (no TLS needed — the
// WapiRequestBuilder only switches to HTTPS when hostCfg.Scheme != "http").
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

	ref := m.seed(&ibclient.ZoneDelegated{
		Fqdn:            "delegated.example.com",
		View:            stringPtr("default"),
		DelegateTo:      ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:         stringPtr("hello"),
		Disable:         boolPtr(false),
		Locked:          boolPtr(false),
		DelegatedTtl:    uint32Ptr(300),
		UseDelegatedTtl: boolPtr(true),
		Ea:              ibclient.EA{"env": "prod"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.Disable = boolPtr(false)
	cr.Spec.ForProvider.Locked = boolPtr(false)
	cr.Spec.ForProvider.DelegatedTTL = uint32Ptr(300)
	cr.Spec.ForProvider.UseDelegatedTTL = boolPtr(true)
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
	if len(cr.Status.AtProvider.DelegateTo) != 1 || *cr.Status.AtProvider.DelegateTo[0].Name != "ns1.example.com" {
		t.Errorf("AtProvider.DelegateTo = %+v, want one entry ns1.example.com", cr.Status.AtProvider.DelegateTo)
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
	cr := newClusterZoneDelegated("my-zone", "zone_delegated/does-not-exist:delegated.example.com/default")

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
	cr := newClusterZoneDelegated("my-zone", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())       // simulate NameAsExternalName initializer

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
	cr := newClusterZoneDelegated("my-zone", "zone_delegated/test1:delegated.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", "zone_delegated/test1:delegated.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, empty strings, a
// nil Ea map, an empty DelegateTo list) must not panic and must produce a
// valid observation with nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneDelegated{})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", ref)

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
	if ap.DelegateTo != nil {
		t.Errorf("AtProvider.DelegateTo = %v, want nil", ap.DelegateTo)
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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", "") // no external-name yet

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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateZoneDelegated) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateZoneDelegated)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty after failed create", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneDelegated{
		Fqdn:       "delegated.example.com",
		View:       stringPtr("original-view"),
		ZoneFormat: "FORWARD",
		DelegateTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", ref)
	// Mutate the immutable fqdn/view/zoneFormat fields in spec — this must
	// NOT affect ResourceUpToDate, since they are excluded from
	// isUpToDate (WAPI has no UpdateZoneDelegated parameter for them).
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

	ref := m.seed(&ibclient.ZoneDelegated{
		Fqdn:       "delegated.example.com",
		View:       stringPtr("default"),
		DelegateTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:    stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", ref)
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

func TestClusterUpdateDoesNotSendImmutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneDelegated{
		Fqdn:       "delegated.example.com",
		View:       stringPtr("default"),
		ZoneFormat: "FORWARD",
		DelegateTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", ref)

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

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", "zone_delegated/test1:delegated.example.com/default")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateZoneDelegated) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateZoneDelegated)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneDelegated{Fqdn: "delegated.example.com", View: stringPtr("default")})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", ref)

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
	cr := newClusterZoneDelegated("my-zone", "zone_delegated/does-not-exist:delegated.example.com/default")

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
	cr := newClusterZoneDelegated("my-zone", "zone_delegated/test1:delegated.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteZoneDelegated) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteZoneDelegated)
	}
}

func TestClusterDeleteForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", "zone_delegated/test1:delegated.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("Delete: expected error for 403, got nil")
	}
}

// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject verifies the
// core defect fix: a 404 against the stored _ref must not be treated as
// "already deleted" when a natural-key search finds the same identity
// still live under a different _ref. Deleting that record would be
// unverifiable ownership, so Delete() must refuse and leave the record in
// place.
func TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.ZoneDelegated{Fqdn: "delegated.example.com", View: stringPtr("default")})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", "zone_delegated/stale-ref:delegated.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// companion happy path: a 404 against the stored _ref, and a natural-key
// search that finds nothing, means the object really is gone.
func TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterZoneDelegated("my-zone", "zone_delegated/stale-ref:delegated.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
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

	cr := newClusterZoneDelegated("my-zone", "")
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

	cr := newClusterZoneDelegated("my-zone", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneDelegated{
		Fqdn:       "delegated.example.com",
		View:       stringPtr("default"),
		DelegateTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", ref, "ProviderConfig")

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
	cr := newNamespacedZoneDelegated("default", "my-zone", "zone_delegated/does-not-exist:delegated.example.com/default", "ProviderConfig")

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
	cr := newNamespacedZoneDelegated("default", "my-zone", "", "ProviderConfig")
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
	cr := newNamespacedZoneDelegated("default", "my-zone", "zone_delegated/test1:delegated.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", "", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateZoneDelegated) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateZoneDelegated)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty after failed create", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneDelegated{
		Fqdn:       "delegated.example.com",
		View:       stringPtr("default"),
		DelegateTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:    stringPtr("old comment"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", ref, "ProviderConfig")
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

// TestNamespacedUpdateError verifies that a WAPI 5xx response during
// Update is propagated (wrapped, not swallowed).
func TestNamespacedUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", "zone_delegated/test1:delegated.example.com/default", "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateZoneDelegated) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateZoneDelegated)
	}
}

// TestNamespacedUpdateDoesNotSendImmutableFields mirrors the cluster-scope
// assertion: fqdn, view, and zone_format must never appear in the
// namespaced-scope Update request body either.
func TestNamespacedUpdateDoesNotSendImmutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.ZoneDelegated{
		Fqdn:       "delegated.example.com",
		View:       stringPtr("default"),
		ZoneFormat: "FORWARD",
		DelegateTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", ref, "ProviderConfig")

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

	ref := m.seed(&ibclient.ZoneDelegated{Fqdn: "delegated.example.com", View: stringPtr("default")})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", ref, "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", "zone_delegated/does-not-exist:delegated.example.com/default", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", "zone_delegated/test1:delegated.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteZoneDelegated) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteZoneDelegated)
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.ZoneDelegated{Fqdn: "delegated.example.com", View: stringPtr("default")})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", "zone_delegated/stale-ref:delegated.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live record was removed despite the refusal — DELETE must not have been issued against it")
	}
}

// TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch is the
// namespaced-scope counterpart of
// TestClusterDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch.
func TestNamespacedDeleteSucceedsWhenStaleRefHasNoNaturalKeyMatch(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedZoneDelegated("default", "my-zone", "zone_delegated/stale-ref:delegated.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
}

// ── namespaced: Disconnect ──────────────────────────────────────────────

func TestNamespacedDisconnectIsNoop(t *testing.T) {
	e := &namespacedExternal{kube: &recordingKubeClient{}}
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

	cr := newNamespacedZoneDelegated(ns, "my-zone", "", "ProviderConfig")
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

	cr := newNamespacedZoneDelegated("app-ns", "my-zone", "", "ClusterProviderConfig")
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

	cr := newNamespacedZoneDelegated("default", "my-zone", "", "SomeOtherKind")
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

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var comment, nsGroup, view, zoneFormat *string
	var disable, locked, useDelegatedTTL *bool
	var delegatedTTL *uint32
	extAttrs := map[string]string(nil)

	rec := &ibclient.ZoneDelegated{
		Comment:         stringPtr("server default"),
		Disable:         boolPtr(true),
		Locked:          boolPtr(true),
		NsGroup:         stringPtr("dns-group"),
		DelegatedTtl:    uint32Ptr(600),
		UseDelegatedTtl: boolPtr(true),
		Ea:              ibclient.EA{"env": "prod"},
		View:            stringPtr("default"),
		ZoneFormat:      "FORWARD",
	}

	changed := lateInitialize(&comment, &nsGroup, &disable, &locked, &useDelegatedTTL, &delegatedTTL, &extAttrs, &view, &zoneFormat, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if comment == nil || *comment != "server default" {
		t.Errorf("lateInitialize: comment = %v, want %q", comment, "server default")
	}
	if disable == nil || *disable != true {
		t.Errorf("lateInitialize: disable = %v, want true", disable)
	}
	if locked == nil || *locked != true {
		t.Errorf("lateInitialize: locked = %v, want true", locked)
	}
	if nsGroup == nil || *nsGroup != "dns-group" {
		t.Errorf("lateInitialize: nsGroup = %v, want %q", nsGroup, "dns-group")
	}
	if delegatedTTL == nil || *delegatedTTL != 600 {
		t.Errorf("lateInitialize: delegatedTTL = %v, want 600", delegatedTTL)
	}
	if useDelegatedTTL == nil || *useDelegatedTTL != true {
		t.Errorf("lateInitialize: useDelegatedTTL = %v, want true", useDelegatedTTL)
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

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	nsGroup := stringPtr("user-group")
	view := stringPtr("user-view")
	zoneFormat := stringPtr("IPV4")
	disable := boolPtr(false)
	locked := boolPtr(false)
	useDelegatedTTL := boolPtr(false)
	delegatedTTL := uint32Ptr(120)
	extAttrs := map[string]string{"env": "staging"}

	rec := &ibclient.ZoneDelegated{
		Comment:         stringPtr("server default"),
		Disable:         boolPtr(true),
		Locked:          boolPtr(true),
		NsGroup:         stringPtr("dns-group"),
		DelegatedTtl:    uint32Ptr(600),
		UseDelegatedTtl: boolPtr(true),
		Ea:              ibclient.EA{"env": "prod"},
		View:            stringPtr("default"),
		ZoneFormat:      "FORWARD",
	}

	changed := lateInitialize(&comment, &nsGroup, &disable, &locked, &useDelegatedTTL, &delegatedTTL, &extAttrs, &view, &zoneFormat, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" || *nsGroup != "user-group" || *view != "user-view" || *zoneFormat != "IPV4" {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
	if *disable != false || *locked != false || *useDelegatedTTL != false || *delegatedTTL != 120 {
		t.Error("lateInitialize: overwrote already-set ForProvider fields")
	}
	if extAttrs["env"] != "staging" {
		t.Error("lateInitialize: overwrote already-set ExtAttrs")
	}
}

// TestLateInitializeDoesNotBackfillDelegatedTTLWhenUseDelegatedTTLOff
// proves that when useDelegatedTtl is false the observed delegatedTtl
// (WAPI's own default, not a value the user's config implies) is never
// written back into spec.forProvider.delegatedTtl.
func TestLateInitializeDoesNotBackfillDelegatedTTLWhenUseDelegatedTTLOff(t *testing.T) {
	var comment, nsGroup, view, zoneFormat *string
	var disable, locked *bool
	useDelegatedTTL := boolPtr(false)
	var delegatedTTL *uint32
	var extAttrs map[string]string

	rec := &ibclient.ZoneDelegated{
		DelegatedTtl:    uint32Ptr(28800),
		UseDelegatedTtl: boolPtr(false),
	}

	lateInitialize(&comment, &nsGroup, &disable, &locked, &useDelegatedTTL, &delegatedTTL, &extAttrs, &view, &zoneFormat, rec)

	if delegatedTTL != nil {
		t.Errorf("lateInitialize: delegatedTtl = %v, want nil (useDelegatedTtl is off, observed delegatedTtl is the server's own default, not a user value)", *delegatedTTL)
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedRecord := func() *ibclient.ZoneDelegated {
		return &ibclient.ZoneDelegated{
			DelegateTo:      ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
			Comment:         stringPtr("hello"),
			Disable:         boolPtr(false),
			Locked:          boolPtr(false),
			NsGroup:         stringPtr("dns-group"),
			DelegatedTtl:    uint32Ptr(300),
			UseDelegatedTtl: boolPtr(true),
			Ea:              ibclient.EA{"env": "prod"},
		}
	}

	cases := map[string]struct {
		reason          string
		delegateTo      []ibclient.NameServer
		comment         *string
		nsGroup         *string
		disable         *bool
		locked          *bool
		useDelegatedTTL *bool
		delegatedTTL    *uint32
		extAttrs        map[string]string
		want            bool
	}{
		"IdenticalFieldsAreUpToDate": {
			reason:          "when every mutable field matches the observed record, the resource must be reported up to date",
			delegateTo:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			disable:         boolPtr(false),
			locked:          boolPtr(false),
			useDelegatedTTL: boolPtr(true),
			delegatedTTL:    uint32Ptr(300),
			extAttrs:        map[string]string{"env": "prod"},
			want:            true,
		},
		"ChangedDelegateToIsNotUpToDate": {
			reason:          "a changed delegateTo list must be detected as drift",
			delegateTo:      []ibclient.NameServer{{Name: "ns2.example.com", Address: "10.0.0.54"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			disable:         boolPtr(false),
			locked:          boolPtr(false),
			useDelegatedTTL: boolPtr(true),
			delegatedTTL:    uint32Ptr(300),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedCommentIsNotUpToDate": {
			reason:          "a changed comment must be detected as drift",
			delegateTo:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("goodbye"),
			nsGroup:         stringPtr("dns-group"),
			disable:         boolPtr(false),
			locked:          boolPtr(false),
			useDelegatedTTL: boolPtr(true),
			delegatedTTL:    uint32Ptr(300),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedDisableIsNotUpToDate": {
			reason:          "a changed disable flag must be detected as drift",
			delegateTo:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			disable:         boolPtr(true),
			locked:          boolPtr(false),
			useDelegatedTTL: boolPtr(true),
			delegatedTTL:    uint32Ptr(300),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedLockedIsNotUpToDate": {
			reason:          "a changed locked flag must be detected as drift",
			delegateTo:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			disable:         boolPtr(false),
			locked:          boolPtr(true),
			useDelegatedTTL: boolPtr(true),
			delegatedTTL:    uint32Ptr(300),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedNsGroupIsNotUpToDate": {
			reason:          "a changed nsGroup must be detected as drift",
			delegateTo:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("other-group"),
			disable:         boolPtr(false),
			locked:          boolPtr(false),
			useDelegatedTTL: boolPtr(true),
			delegatedTTL:    uint32Ptr(300),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedDelegatedTTLIsNotUpToDate": {
			reason:          "a changed delegatedTtl must be detected as drift",
			delegateTo:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			disable:         boolPtr(false),
			locked:          boolPtr(false),
			useDelegatedTTL: boolPtr(true),
			delegatedTTL:    uint32Ptr(600),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ChangedUseDelegatedTTLIsNotUpToDate": {
			reason:          "a changed useDelegatedTtl flag must be detected as drift",
			delegateTo:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			disable:         boolPtr(false),
			locked:          boolPtr(false),
			useDelegatedTTL: boolPtr(false),
			delegatedTTL:    uint32Ptr(300),
			extAttrs:        map[string]string{"env": "prod"},
			want:            false,
		},
		"ExtAttrsDifferentValueIsNotUpToDate": {
			reason:          "an extAttrs value change on an existing key must be detected as drift",
			delegateTo:      []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
			comment:         stringPtr("hello"),
			nsGroup:         stringPtr("dns-group"),
			disable:         boolPtr(false),
			locked:          boolPtr(false),
			useDelegatedTTL: boolPtr(true),
			delegatedTTL:    uint32Ptr(300),
			extAttrs:        map[string]string{"env": "staging"},
			want:            false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := isUpToDate(tc.delegateTo, tc.comment, tc.nsGroup, tc.disable, tc.locked, tc.useDelegatedTTL, tc.delegatedTTL, tc.extAttrs, observedRecord())
			if got != tc.want {
				t.Errorf("%s: isUpToDate() = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

// TestIsUpToDateIgnoresDelegatedTTLWhenUseDelegatedTTLOff proves the
// delegatedTtl comparison is gated on useDelegatedTtl. When it is false,
// WAPI ignores the submitted delegatedTtl and returns its own default (a
// realistic non-zero value, not 0) on every GET — the spec value and the
// observed value are unrelated quantities, and comparing them
// unconditionally can never converge.
func TestIsUpToDateIgnoresDelegatedTTLWhenUseDelegatedTTLOff(t *testing.T) {
	observed := &ibclient.ZoneDelegated{
		DelegateTo:      ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:         stringPtr("hello"),
		DelegatedTtl:    uint32Ptr(28800),
		UseDelegatedTtl: boolPtr(false),
		Ea:              ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		[]ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
		stringPtr("hello"),
		nil,
		nil,
		nil,
		boolPtr(false),
		uint32Ptr(0),
		map[string]string{"env": "prod"},
		observed,
	)
	if !got {
		t.Error("isUpToDate: want true when useDelegatedTtl is off and only the server-owned delegatedTtl differs, got false (non-convergent drift comparison)")
	}
}

// TestIsUpToDateDetectsUseDelegatedTTLTransition proves a
// useDelegatedTtl true -> false transition is still detected as drift
// even though the value comparison is gated off. The flag comparison
// must be unconditional.
func TestIsUpToDateDetectsUseDelegatedTTLTransition(t *testing.T) {
	observed := &ibclient.ZoneDelegated{
		DelegateTo:      ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
		Comment:         stringPtr("hello"),
		DelegatedTtl:    uint32Ptr(300),
		UseDelegatedTtl: boolPtr(true),
		Ea:              ibclient.EA{"env": "prod"},
	}

	got := isUpToDate(
		[]ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}},
		stringPtr("hello"),
		nil,
		nil,
		nil,
		boolPtr(false),
		uint32Ptr(300),
		map[string]string{"env": "prod"},
		observed,
	)
	if got {
		t.Error("isUpToDate: want false on a useDelegatedTtl true -> false transition, got true (drift not detected)")
	}
}

func TestIsUpToDateExtAttrsEmptyVsNil(t *testing.T) {
	rec := &ibclient.ZoneDelegated{
		DelegateTo: ibclient.NullableNameServers{NameServers: []ibclient.NameServer{{Name: "ns1.example.com", Address: "10.0.0.53"}}},
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

// ── NameServer conversion: round-trip ───────────────────────────────────

func TestClusterDelegateToRoundTrip(t *testing.T) {
	in := []clusterv1alpha1.ZoneDelegatedNameServer{
		{Name: stringPtr("ns1.example.com"), Address: stringPtr("10.0.0.53")},
		{Name: stringPtr("ns2.example.com"), Address: stringPtr("10.0.0.54")},
	}
	sdk := clusterDelegateToSDK(in)
	out := clusterDelegateFromSDK(sdk)
	if len(out) != len(in) {
		t.Fatalf("round-trip: got %d entries, want %d", len(out), len(in))
	}
	for i := range in {
		if *out[i].Name != *in[i].Name || *out[i].Address != *in[i].Address {
			t.Errorf("round-trip[%d]: got %+v, want %+v", i, out[i], in[i])
		}
	}
}

func TestNamespacedDelegateToRoundTrip(t *testing.T) {
	in := []namespacedv1alpha1.ZoneDelegatedNameServer{
		{Name: stringPtr("ns1.example.com"), Address: stringPtr("10.0.0.53")},
	}
	sdk := namespacedDelegateToSDK(in)
	out := namespacedDelegateFromSDK(sdk)
	if len(out) != 1 || *out[0].Name != "ns1.example.com" || *out[0].Address != "10.0.0.53" {
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
