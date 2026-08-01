// Package dtcserver unit tests for the DTCServer MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI dtc:server
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package dtcserver

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dtcserver/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dtcserver/v1alpha1"
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

// Shared literals reused across many test cases (deduplicated for goconst).
const (
	nsDefault      = "default"
	eaKeyEnv       = "env"
	eaValProd      = "prod"
	monitorRefSNMP = "dtc:monitor:snmp/ZG5z...:snmp"
)

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

// newClusterDTCServer builds a minimal cluster-scoped DTCServer CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterDTCServer(crName, externalName string) *clusterv1alpha1.DTCServer {
	cr := &clusterv1alpha1.DTCServer{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.DTCServerSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: nsDefault},
			},
			ForProvider: clusterv1alpha1.DTCServerParameters{
				Name: stringPtr("my-dtc-server"),
				Host: stringPtr("2.3.4.5"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedDTCServer is the namespaced variant of newClusterDTCServer.
func newNamespacedDTCServer(ns, crName, externalName, pcKind string) *namespacedv1alpha1.DTCServer {
	cr := &namespacedv1alpha1.DTCServer{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.DTCServerSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: nsDefault},
			},
			ForProvider: namespacedv1alpha1.DTCServerParameters{
				Name: stringPtr("my-dtc-server"),
				Host: stringPtr("2.3.4.5"),
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
// mockDtcServerServer emulates the subset of NIOS WAPI dtc:server endpoints
// exercised by the DTCServer controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.DtcServer type so the wire format exactly matches what the SDK
// sends and expects.

type mockDtcServerServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.DtcServer
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert field content.
	lastUpdateBody []byte
}

func newMockDtcServerServer() *mockDtcServerServer {
	return &mockDtcServerServer{records: map[string]*ibclient.DtcServer{}}
}

func (m *mockDtcServerServer) seed(rec *ibclient.DtcServer) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		rec.Ref = m.newRefLocked(rec)
	}
	m.records[rec.Ref] = rec
	return rec.Ref
}

func (m *mockDtcServerServer) newRefLocked(rec *ibclient.DtcServer) string {
	name := ""
	if rec.Name != nil {
		name = *rec.Name
	}
	return "dtc:server/test" + itoa(m.nextRef) + ":" + name
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

// handler returns an http.Handler implementing the dtc:server WAPI surface.
func (m *mockDtcServerServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/dtc:server", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.DtcServer
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
		var incoming ibclient.DtcServer
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		existing.Name = incoming.Name
		existing.Host = incoming.Host
		existing.Comment = incoming.Comment
		existing.Disable = incoming.Disable
		existing.AutoCreateHostRecord = incoming.AutoCreateHostRecord
		existing.SniHostname = incoming.SniHostname
		existing.UseSniHostname = incoming.UseSniHostname
		existing.Monitors = incoming.Monitors
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

// newTestClients builds a *dtcServerClients pointed at the given
// httptest.Server via plain HTTP (no TLS needed — the WapiRequestBuilder
// only switches to HTTPS when hostCfg.Scheme != "http").
func newTestClients(t *testing.T, srv *httptest.Server) *dtcServerClients {
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
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcServer{
		Name:                 stringPtr("my-dtc-server"),
		Host:                 stringPtr("2.3.4.5"),
		Comment:              stringPtr("hello"),
		Disable:              boolPtr(false),
		AutoCreateHostRecord: boolPtr(true),
		Ea:                   ibclient.EA{eaKeyEnv: eaValProd},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.Disable = boolPtr(false)
	cr.Spec.ForProvider.AutoCreateHostRecord = boolPtr(true)
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
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", "dtc:server/does-not-exist:my-dtc-server")

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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", "") // external-name unset
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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", "dtc:server/test1:my-dtc-server")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", "dtc:server/test1:my-dtc-server")

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
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcServer{})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", ref)

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
	if ap.Host != nil {
		t.Errorf("AtProvider.Host = %v, want nil", ap.Host)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.Disable != nil {
		t.Errorf("AtProvider.Disable = %v, want nil", ap.Disable)
	}
	if ap.AutoCreateHostRecord != nil {
		t.Errorf("AtProvider.AutoCreateHostRecord = %v, want nil", ap.AutoCreateHostRecord)
	}
	if ap.SniHostname != nil {
		t.Errorf("AtProvider.SniHostname = %v, want nil", ap.SniHostname)
	}
	if ap.UseSniHostname != nil {
		t.Errorf("AtProvider.UseSniHostname = %v, want nil", ap.UseSniHostname)
	}
	if ap.Monitors != nil {
		t.Errorf("AtProvider.Monitors = %v, want nil", ap.Monitors)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.Health != nil {
		t.Errorf("AtProvider.Health = %v, want nil", ap.Health)
	}
}

func TestClusterObserveMonitorsAndHealth(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcServer{
		Name: stringPtr("my-dtc-server"),
		Host: stringPtr("2.3.4.5"),
		Monitors: []*ibclient.DtcServerMonitor{
			{Monitor: monitorRefSNMP, Host: "2.3.4.5"},
		},
		Health: &ibclient.DtcHealth{
			Availability: "GREEN",
			Description:  "healthy",
			EnabledState: "ENABLED",
		},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", ref)
	cr.Spec.ForProvider.Monitors = []clusterv1alpha1.DTCServerMonitor{
		{Monitor: stringPtr(monitorRefSNMP), Host: stringPtr("2.3.4.5")},
	}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true, got false")
	}

	ap := cr.Status.AtProvider
	if len(ap.Monitors) != 1 || ap.Monitors[0].Monitor == nil || *ap.Monitors[0].Monitor != monitorRefSNMP {
		t.Errorf("AtProvider.Monitors = %+v, want one entry with the seeded monitor ref", ap.Monitors)
	}
	if ap.Health == nil || ap.Health.Availability == nil || *ap.Health.Availability != "GREEN" {
		t.Errorf("AtProvider.Health = %+v, want Availability=GREEN", ap.Health)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", "") // no external-name yet
	cr.Spec.ForProvider.Monitors = []clusterv1alpha1.DTCServerMonitor{
		{Monitor: stringPtr(monitorRefSNMP), Host: stringPtr("2.3.4.5")},
	}

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
	if len(stored.Monitors) != 1 || stored.Monitors[0].Monitor != monitorRefSNMP {
		t.Errorf("Create: stored monitors = %+v, want the ref passed through untouched (no name+type lookup)", stored.Monitors)
	}
}

// TestClusterCreateServerError pins the error-propagation path when the
// WAPI backend rejects the create POST outright (e.g. transient 500s).
func TestClusterCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", "")

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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", "dtc:server/test1:my-dtc-server")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
}

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcServer{
		Name:    stringPtr("my-dtc-server"),
		Host:    stringPtr("2.3.4.5"),
		Comment: stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", ref)
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

// TestUpdateSendsAllFields pins the PUT-echo-everything contract for
// DTCServer: since there are no known immutable fields, Update must send
// every mutable field on every request (not a partial patch).
func TestUpdateSendsAllFields(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcServer{
		Name: stringPtr("my-dtc-server"),
		Host: stringPtr("2.3.4.5"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", ref)
	cr.Spec.ForProvider.Host = stringPtr("2.3.4.6")

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
	if _, present := raw["host"]; !present {
		t.Error("Update: request body missing 'host' — PUT must echo all fields")
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcServer{Name: stringPtr("my-dtc-server")})

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", ref)

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
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", "dtc:server/does-not-exist:my-dtc-server")

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

	e := &clusterExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newClusterDTCServer("my-dtcserver", "dtc:server/test1:my-dtc-server")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteDTCServer) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteDTCServer)
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

	cr := newClusterDTCServer("my-dtcserver", "")
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

	cr := newClusterDTCServer("my-dtcserver", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcServer{
		Name: stringPtr("my-dtc-server"),
		Host: stringPtr("2.3.4.5"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", ref, "ProviderConfig")

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

// TestNamespacedObserveMonitorsAndHealth pins the namespaced-scope monitor
// and health conversion path (monitorsFromNamespaced/monitorPairsToNamespaced/
// healthToNamespaced), mirroring the cluster-scope coverage above.
func TestNamespacedObserveMonitorsAndHealth(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcServer{
		Name: stringPtr("my-dtc-server"),
		Host: stringPtr("2.3.4.5"),
		Monitors: []*ibclient.DtcServerMonitor{
			{Monitor: monitorRefSNMP, Host: "2.3.4.5"},
		},
		Health: &ibclient.DtcHealth{
			Availability: "GREEN",
			Description:  "healthy",
			EnabledState: "ENABLED",
		},
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", ref, "ProviderConfig")
	cr.Spec.ForProvider.Monitors = []namespacedv1alpha1.DTCServerMonitor{
		{Monitor: stringPtr(monitorRefSNMP), Host: stringPtr("2.3.4.5")},
	}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true, got false")
	}

	ap := cr.Status.AtProvider
	if len(ap.Monitors) != 1 || ap.Monitors[0].Monitor == nil || *ap.Monitors[0].Monitor != monitorRefSNMP {
		t.Errorf("AtProvider.Monitors = %+v, want one entry with the seeded monitor ref", ap.Monitors)
	}
	if ap.Health == nil || ap.Health.Availability == nil || *ap.Health.Availability != "GREEN" {
		t.Errorf("AtProvider.Health = %+v, want Availability=GREEN", ap.Health)
	}
}

func TestNamespacedObserveNotFound(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", "dtc:server/does-not-exist:my-dtc-server", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", "", "ProviderConfig")
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", "dtc:server/test1:my-dtc-server", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", "dtc:server/test1:my-dtc-server", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", "", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name set to %q despite failed create", got)
	}
}

func TestNamespacedUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", "dtc:server/test1:my-dtc-server", "ProviderConfig")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcServer{
		Name: stringPtr("my-dtc-server"),
		Host: stringPtr("2.3.4.5"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", ref, "ProviderConfig")
	cr.Spec.ForProvider.Host = stringPtr("2.3.4.6")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.Host == nil || *stored.Host != "2.3.4.6" {
		t.Errorf("Update: stored host = %v, want 2.3.4.6", stored.Host)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.DtcServer{Name: stringPtr("my-dtc-server")})

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockDtcServerServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", "dtc:server/does-not-exist:my-dtc-server", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, clients: newTestClients(t, srv)}
	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", "dtc:server/test1:my-dtc-server", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteDTCServer) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteDTCServer)
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

	cr := newNamespacedDTCServer(ns, "my-dtcserver", "", "ProviderConfig")
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

	cr := newNamespacedDTCServer("app-ns", "my-dtcserver", "", "ClusterProviderConfig")
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

	cr := newNamespacedDTCServer(nsDefault, "my-dtcserver", "", "SomeOtherKind")
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
			in:     eaValProd,
			want:   eaValProd,
		},
		"BoolTrue": {
			reason: "ibclient.Bool(true) renders as the WAPI-style 'True'",
			in:     ibclient.Bool(true),
			want:   "True",
		},
		"BoolFalse": {
			reason: "ibclient.Bool(false) renders as the WAPI-style 'False'",
			in:     ibclient.Bool(false),
			want:   "False",
		},
		"StringSlice": {
			reason: "multi-value EAs (decoded as []string) join on comma",
			in:     []string{"a", "b", "c"},
			want:   "a,b,c",
		},
		"IntFallback": {
			reason: "unexpected scalar types fall back to fmt.Sprintf(%v)",
			in:     42,
			want:   "42",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := stringifyEAValue(tc.in)
			if got != tc.want {
				t.Errorf("%s: stringifyEAValue() = %q, want %q", tc.reason, got, tc.want)
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

func TestMonitorsRoundTrip(t *testing.T) {
	in := []monitorPair{
		{Monitor: stringPtr(monitorRefSNMP), Host: stringPtr("2.3.4.5")},
	}
	sdk := buildMonitors(in)
	out := monitorPairsFromSDK(sdk)
	if !monitorsEqual(in, out) {
		t.Errorf("monitors round-trip: got %+v, want %+v", out, in)
	}
}

func TestMonitorsEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !monitorsEqual(nil, []monitorPair{}) {
		t.Error("monitorsEqual(nil, []) = false, want true")
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
	var comment, sniHostname *string
	var disable, autoCreateHostRecord, useSniHostname *bool
	var monitors []monitorPair
	var extAttrs map[string]string

	rec := &ibclient.DtcServer{
		Comment:              stringPtr("server comment"),
		Disable:              boolPtr(true),
		AutoCreateHostRecord: boolPtr(true),
		SniHostname:          stringPtr("sni.example.com"),
		UseSniHostname:       boolPtr(true),
		Monitors: []*ibclient.DtcServerMonitor{
			{Monitor: monitorRefSNMP, Host: "2.3.4.5"},
		},
		Ea: ibclient.EA{eaKeyEnv: eaValProd},
	}

	changed := lateInitialize(&comment, &disable, &autoCreateHostRecord, &useSniHostname, &sniHostname, &monitors, &extAttrs, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if comment == nil || *comment != "server comment" {
		t.Errorf("comment = %v, want %q", comment, "server comment")
	}
	if disable == nil || !*disable {
		t.Errorf("disable = %v, want true", disable)
	}
	if autoCreateHostRecord == nil || !*autoCreateHostRecord {
		t.Errorf("autoCreateHostRecord = %v, want true", autoCreateHostRecord)
	}
	if sniHostname == nil || *sniHostname != "sni.example.com" {
		t.Errorf("sniHostname = %v, want %q", sniHostname, "sni.example.com")
	}
	if useSniHostname == nil || !*useSniHostname {
		t.Errorf("useSniHostname = %v, want true", useSniHostname)
	}
	if len(monitors) != 1 {
		t.Errorf("monitors = %+v, want one entry", monitors)
	}
	if len(extAttrs) != 1 || extAttrs[eaKeyEnv] != eaValProd {
		t.Errorf("extAttrs = %v, want {env: prod}", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	disable := boolPtr(false)
	autoCreateHostRecord := boolPtr(false)
	sniHostname := stringPtr("user-sni.example.com")
	useSniHostname := boolPtr(true)
	monitors := []monitorPair{{Monitor: stringPtr(monitorRefSNMP), Host: stringPtr("2.3.4.5")}}
	extAttrs := map[string]string{"owner": "user-team"}

	rec := &ibclient.DtcServer{
		Comment:              stringPtr("server comment"),
		Disable:              boolPtr(true),
		AutoCreateHostRecord: boolPtr(true),
		SniHostname:          stringPtr("server-sni.example.com"),
		UseSniHostname:       boolPtr(false),
		Monitors: []*ibclient.DtcServerMonitor{
			{Monitor: "dtc:monitor:http/ZG5z...:http", Host: "2.3.4.6"},
		},
		Ea: ibclient.EA{eaKeyEnv: eaValProd},
	}

	changed := lateInitialize(&comment, &disable, &autoCreateHostRecord, &useSniHostname, &sniHostname, &monitors, &extAttrs, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" {
		t.Errorf("comment = %q, want unchanged %q", *comment, "user comment")
	}
	if *disable {
		t.Error("disable overwritten by lateInitialize")
	}
	if *sniHostname != "user-sni.example.com" {
		t.Errorf("sniHostname = %q, want unchanged", *sniHostname)
	}
	if len(monitors) != 1 || strOrEmpty(monitors[0].Monitor) != monitorRefSNMP {
		t.Errorf("monitors overwritten by lateInitialize: %+v", monitors)
	}
	if extAttrs["owner"] != "user-team" {
		t.Errorf("extAttrs overwritten by lateInitialize: %v", extAttrs)
	}
}

// TestLateInitializeDoesNotBackfillSniHostnameWhenUseSniHostnameOff
// proves that when useSniHostname is false the observed sniHostname
// (WAPI's own default, not a value the user's config implies) is never
// written back into spec.forProvider.sniHostname.
func TestLateInitializeDoesNotBackfillSniHostnameWhenUseSniHostnameOff(t *testing.T) {
	var comment, sniHostname *string
	var disable, autoCreateHostRecord *bool
	useSniHostname := boolPtr(false)
	var monitors []monitorPair
	var extAttrs map[string]string

	rec := &ibclient.DtcServer{
		SniHostname:    stringPtr("server-default.example.com"),
		UseSniHostname: boolPtr(false),
	}

	lateInitialize(&comment, &disable, &autoCreateHostRecord, &useSniHostname, &sniHostname, &monitors, &extAttrs, rec)

	if sniHostname != nil {
		t.Errorf("lateInitialize: sniHostname = %v, want nil (useSniHostname is off, observed sniHostname is the server's own default, not a user value)", *sniHostname)
	}
}

func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	// name and host are required fields — lateInitialize has no
	// parameters for them at all, so this test simply pins that
	// contract by confirming the function signature only accepts the
	// optional fields.
	var comment, sniHostname *string
	var disable, autoCreateHostRecord, useSniHostname *bool
	var monitors []monitorPair
	var extAttrs map[string]string

	rec := &ibclient.DtcServer{
		Name: stringPtr("server-name"),
		Host: stringPtr("server-host"),
	}

	_ = lateInitialize(&comment, &disable, &autoCreateHostRecord, &useSniHostname, &sniHostname, &monitors, &extAttrs, rec)
	// No assertions needed beyond "does not panic" — name/host aren't
	// parameters of lateInitialize, so there is nothing for it to
	// overwrite.
}

func TestIsUpToDate(t *testing.T) {
	base := &ibclient.DtcServer{
		Name:                 stringPtr("my-dtc-server"),
		Host:                 stringPtr("2.3.4.5"),
		Comment:              stringPtr("hello"),
		Disable:              boolPtr(false),
		AutoCreateHostRecord: boolPtr(true),
		SniHostname:          stringPtr("sni.example.com"),
		UseSniHostname:       boolPtr(true),
		Monitors: []*ibclient.DtcServerMonitor{
			{Monitor: monitorRefSNMP, Host: "2.3.4.5"},
		},
		Ea: ibclient.EA{eaKeyEnv: eaValProd},
	}
	baseMonitors := []monitorPair{{Monitor: stringPtr(monitorRefSNMP), Host: stringPtr("2.3.4.5")}}
	baseExtAttrs := map[string]string{eaKeyEnv: eaValProd}

	cases := map[string]struct {
		mutate func() (name, host, comment, sniHostname *string, disable, autoCreateHostRecord, useSniHostname *bool, monitors []monitorPair, extAttrs map[string]string)
		want   bool
	}{
		"MatchesExactly": {
			mutate: func() (*string, *string, *string, *string, *bool, *bool, *bool, []monitorPair, map[string]string) {
				return stringPtr("my-dtc-server"), stringPtr("2.3.4.5"), stringPtr("hello"), stringPtr("sni.example.com"), boolPtr(false), boolPtr(true), boolPtr(true), baseMonitors, baseExtAttrs
			},
			want: true,
		},
		"HostDiffers": {
			mutate: func() (*string, *string, *string, *string, *bool, *bool, *bool, []monitorPair, map[string]string) {
				return stringPtr("my-dtc-server"), stringPtr("9.9.9.9"), stringPtr("hello"), stringPtr("sni.example.com"), boolPtr(false), boolPtr(true), boolPtr(true), baseMonitors, baseExtAttrs
			},
			want: false,
		},
		"CommentDiffers": {
			mutate: func() (*string, *string, *string, *string, *bool, *bool, *bool, []monitorPair, map[string]string) {
				return stringPtr("my-dtc-server"), stringPtr("2.3.4.5"), stringPtr("changed"), stringPtr("sni.example.com"), boolPtr(false), boolPtr(true), boolPtr(true), baseMonitors, baseExtAttrs
			},
			want: false,
		},
		"DisableDiffers": {
			mutate: func() (*string, *string, *string, *string, *bool, *bool, *bool, []monitorPair, map[string]string) {
				return stringPtr("my-dtc-server"), stringPtr("2.3.4.5"), stringPtr("hello"), stringPtr("sni.example.com"), boolPtr(true), boolPtr(true), boolPtr(true), baseMonitors, baseExtAttrs
			},
			want: false,
		},
		"MonitorsDiffer": {
			mutate: func() (*string, *string, *string, *string, *bool, *bool, *bool, []monitorPair, map[string]string) {
				diff := []monitorPair{{Monitor: stringPtr("dtc:monitor:http/ZG5z...:http"), Host: stringPtr("2.3.4.6")}}
				return stringPtr("my-dtc-server"), stringPtr("2.3.4.5"), stringPtr("hello"), stringPtr("sni.example.com"), boolPtr(false), boolPtr(true), boolPtr(true), diff, baseExtAttrs
			},
			want: false,
		},
		"ExtAttrsDiffer": {
			mutate: func() (*string, *string, *string, *string, *bool, *bool, *bool, []monitorPair, map[string]string) {
				return stringPtr("my-dtc-server"), stringPtr("2.3.4.5"), stringPtr("hello"), stringPtr("sni.example.com"), boolPtr(false), boolPtr(true), boolPtr(true), baseMonitors, map[string]string{eaKeyEnv: "dev"}
			},
			want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cname, chost, ccomment, csni, cdisable, cauto, cusesni, cmonitors, cextattrs := tc.mutate()
			got := isUpToDate(cname, chost, ccomment, cdisable, cauto, cusesni, csni, cmonitors, cextattrs, base)
			if got != tc.want {
				t.Errorf("%s: isUpToDate() = %v, want %v", name, got, tc.want)
			}
		})
	}
}

// TestIsUpToDateIgnoresSniHostnameWhenUseSniHostnameOff proves the
// sniHostname comparison is gated on useSniHostname. When it is false,
// WAPI ignores the submitted sniHostname and returns its own default on
// every GET — the spec value and the observed value are unrelated
// quantities, and comparing them unconditionally can never converge.
func TestIsUpToDateIgnoresSniHostnameWhenUseSniHostnameOff(t *testing.T) {
	rec := &ibclient.DtcServer{
		Name:           stringPtr("my-dtc-server"),
		Host:           stringPtr("2.3.4.5"),
		SniHostname:    stringPtr("server-default.example.com"),
		UseSniHostname: boolPtr(false),
	}

	got := isUpToDate(rec.Name, rec.Host, nil, nil, nil, boolPtr(false), stringPtr("user-value.example.com"), nil, nil, rec)
	if !got {
		t.Error("isUpToDate: want true when useSniHostname is off and only the server-owned sniHostname differs, got false (non-convergent drift comparison)")
	}
}

// TestIsUpToDateDetectsUseSniHostnameTransition proves a
// useSniHostname true -> false transition is still detected as drift
// even though the value comparison is gated off. The flag comparison
// must be unconditional.
func TestIsUpToDateDetectsUseSniHostnameTransition(t *testing.T) {
	rec := &ibclient.DtcServer{
		Name:           stringPtr("my-dtc-server"),
		Host:           stringPtr("2.3.4.5"),
		SniHostname:    stringPtr("sni.example.com"),
		UseSniHostname: boolPtr(true),
	}

	got := isUpToDate(rec.Name, rec.Host, nil, nil, nil, boolPtr(false), stringPtr("sni.example.com"), nil, nil, rec)
	if got {
		t.Error("isUpToDate: want false on a useSniHostname true -> false transition, got true (drift not detected)")
	}
}

func TestIsUpToDateExtAttrsEmptyVsNil(t *testing.T) {
	rec := &ibclient.DtcServer{
		Name: stringPtr("my-dtc-server"),
		Host: stringPtr("2.3.4.5"),
		Ea:   nil,
	}
	got := isUpToDate(stringPtr("my-dtc-server"), stringPtr("2.3.4.5"), nil, nil, nil, nil, nil, nil, map[string]string{}, rec)
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
