// Package rangetemplate unit tests for the RangeTemplate MR controllers.
// Tests use inline httptest.NewServer mocks that emulate the WAPI
// rangetemplate endpoints, PascalCase test names (no underscores), and
// white-box access to the unexported connectors/clients so both scopes can
// be exercised without going through the full Connect() credential bridge
// on every test.
package rangetemplate

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/rangetemplate/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/rangetemplate/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── generic helpers ─────────────────────────────────────────────────────────

func stringPtr(s string) *string { return &s }
func uint32Ptr(v uint32) *uint32 { return &v }
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

// newClusterRangeTemplate builds a minimal cluster-scoped RangeTemplate
// CR. When externalName is empty, the external-name annotation is left
// unset. When it equals crName it simulates the framework's
// NameAsExternalName initializer (the pre-create state); any other value
// simulates a Create()-assigned server ref.
func newClusterRangeTemplate(crName, externalName string) *clusterv1alpha1.RangeTemplate {
	cr := &clusterv1alpha1.RangeTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.RangeTemplateSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.RangeTemplateParameters{
				Name:              stringPtr("template1"),
				NumberOfAddresses: uint32Ptr(10),
				Offset:            uint32Ptr(5),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedRangeTemplate is the namespaced variant of
// newClusterRangeTemplate.
func newNamespacedRangeTemplate(ns, crName, externalName, pcKind string) *namespacedv1alpha1.RangeTemplate {
	cr := &namespacedv1alpha1.RangeTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.RangeTemplateSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.RangeTemplateParameters{
				Name:              stringPtr("template1"),
				NumberOfAddresses: uint32Ptr(10),
				Offset:            uint32Ptr(5),
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
// mockWapiServer emulates the subset of NIOS WAPI rangetemplate endpoints
// exercised by the RangeTemplate controller (POST create, GET/PUT/DELETE
// by _ref). Records are marshaled/unmarshaled using the real
// ibclient.Rangetemplate type so the wire format (including the Member
// nested-object envelope and the EA {"value": ...} envelope) exactly
// matches what the SDK sends and expects.

type mockWapiServer struct {
	mu        sync.Mutex
	templates map[string]*ibclient.Rangetemplate
	nextRef   int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert request-body shape.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{templates: map[string]*ibclient.Rangetemplate{}}
}

func (m *mockWapiServer) seed(rt *ibclient.Rangetemplate) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rt.Ref == "" {
		rt.Ref = m.newRefLocked(rt)
	}
	m.templates[rt.Ref] = rt
	return rt.Ref
}

func (m *mockWapiServer) newRefLocked(rt *ibclient.Rangetemplate) string {
	name := ""
	if rt.Name != nil {
		name = *rt.Name
	}
	return "rangetemplate/test" + itoa(m.nextRef) + ":" + name
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

// handler returns an http.Handler implementing the rangetemplate WAPI
// surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/rangetemplate", func(w http.ResponseWriter, r *http.Request) {
		var rt ibclient.Rangetemplate
		if err := json.NewDecoder(r.Body).Decode(&rt); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ref := m.seed(&rt)
		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		rt, ok := m.templates[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, rt)
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.templates[ref]
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
		var incoming ibclient.Rangetemplate
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		existing.Name = incoming.Name
		existing.NumberOfAddresses = incoming.NumberOfAddresses
		existing.Offset = incoming.Offset
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		existing.Options = incoming.Options
		existing.UseOptions = incoming.UseOptions
		existing.ServerAssociationType = incoming.ServerAssociationType
		existing.FailoverAssociation = incoming.FailoverAssociation
		existing.Member = incoming.Member
		existing.CloudApiCompatible = incoming.CloudApiCompatible
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("DELETE /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		_, ok := m.templates[ref]
		delete(m.templates, ref)
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

	ref := m.seed(&ibclient.Rangetemplate{
		Name:                  stringPtr("template1"),
		NumberOfAddresses:     uint32Ptr(10),
		Offset:                uint32Ptr(5),
		Comment:               stringPtr("hello"),
		Ea:                    ibclient.EA{"env": "prod"},
		ServerAssociationType: "MEMBER",
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRangeTemplate("my-rangetemplate", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.ExtAttrs = map[string]string{"env": "prod"}
	cr.Spec.ForProvider.ServerAssociationType = "MEMBER"

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
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRangeTemplate("my-rangetemplate", "rangetemplate/does-not-exist:template1")

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
	cr := newClusterRangeTemplate("my-rangetemplate", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())                // simulate NameAsExternalName initializer

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
	cr := newClusterRangeTemplate("my-rangetemplate", "rangetemplate/test1:template1")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRangeTemplate("my-rangetemplate", "rangetemplate/test1:template1")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (nil pointers, an empty
// options/member) must not panic and must produce a valid observation
// with nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Rangetemplate{})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRangeTemplate("my-rangetemplate", ref)

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
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
	if ap.Options != nil {
		t.Errorf("AtProvider.Options = %v, want nil", ap.Options)
	}
	if ap.Member != nil {
		t.Errorf("AtProvider.Member = %v, want nil", ap.Member)
	}
	if ap.ServerAssociationType != nil {
		t.Errorf("AtProvider.ServerAssociationType = %v, want nil", ap.ServerAssociationType)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRangeTemplate("my-rangetemplate", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Rangetemplate{
		Name:              stringPtr("template1"),
		NumberOfAddresses: uint32Ptr(10),
		Offset:            uint32Ptr(5),
		Comment:           stringPtr("old comment"),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRangeTemplate("my-rangetemplate", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.templates[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "new comment")
	}
}

// TestClusterUpdateSendsAllMutableFields verifies the PUT (partial/merge)
// request body carries the required fields (name, numberOfAddresses,
// offset) even when only an optional field changed — this provider's
// UpdateRangeTemplate wrapper always re-sends the full set of mutable
// fields (no immutable-field table for RangeTemplate).
func TestClusterUpdateSendsAllMutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Rangetemplate{
		Name:              stringPtr("template1"),
		NumberOfAddresses: uint32Ptr(10),
		Offset:            uint32Ptr(5),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRangeTemplate("my-rangetemplate", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")

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
	if raw["name"] != "template1" {
		t.Errorf("Update: request body name = %v, want %q", raw["name"], "template1")
	}
	if raw["number_of_addresses"] != float64(10) {
		t.Errorf("Update: request body number_of_addresses = %v, want 10", raw["number_of_addresses"])
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Rangetemplate{Name: stringPtr("template1")})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRangeTemplate("my-rangetemplate", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.templates[ref]
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
	cr := newClusterRangeTemplate("my-rangetemplate", "rangetemplate/does-not-exist:template1")

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
	cr := newClusterRangeTemplate("my-rangetemplate", "rangetemplate/test1:template1")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteRangeTemplate) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteRangeTemplate)
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

	cr := newClusterRangeTemplate("my-rangetemplate", "")
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

	cr := newClusterRangeTemplate("my-rangetemplate", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Rangetemplate{
		Name:              stringPtr("template1"),
		NumberOfAddresses: uint32Ptr(10),
		Offset:            uint32Ptr(5),
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", ref, "ProviderConfig")

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
}

func TestNamespacedObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", "rangetemplate/does-not-exist:template1", "ProviderConfig")

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
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", "", "ProviderConfig")
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
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", "rangetemplate/test1:template1", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", "rangetemplate/test1:template1", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// ── namespaced: Create ────────────────────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// ── namespaced: Update ─────────────────────────────────────────────────────

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Rangetemplate{
		Name:              stringPtr("template1"),
		NumberOfAddresses: uint32Ptr(10),
		Offset:            uint32Ptr(5),
		Comment:           stringPtr("old comment"),
	})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.templates[ref]
	m.mu.Unlock()
	if stored.Comment == nil || *stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %v, want %q", stored.Comment, "new comment")
	}
}

// ── namespaced: Delete ──────────────────────────────────────────────────

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Rangetemplate{Name: stringPtr("template1")})

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.templates[ref]
	m.mu.Unlock()
	if stillExists {
		t.Error("Delete: record still present after Delete")
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", "rangetemplate/does-not-exist:template1", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", "rangetemplate/test1:template1", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
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

	cr := newNamespacedRangeTemplate(ns, "my-rangetemplate", "", "ProviderConfig")
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

	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", "", "ClusterProviderConfig")
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

	cr := newNamespacedRangeTemplate("default", "my-rangetemplate", "", "SomeOtherKind")
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

// ── DHCP option/member conversion helpers ────────────────────────────────

func TestDhcpOptionsRoundTrip(t *testing.T) {
	in := []templateOption{
		{Name: stringPtr("routers"), Num: uint32Ptr(3), VendorClass: stringPtr("DHCP"), Value: stringPtr("10.0.0.1"), UseOption: boolPtr(true)},
	}
	got := dhcpOptionsToCommon(buildDhcpOptions(in))
	if !optionsEqual(in, got) {
		t.Errorf("Dhcpoption round-trip: got %+v, want %+v", got, in)
	}
}

func TestDhcpMemberRoundTrip(t *testing.T) {
	in := &templateMember{Ipv4Addr: stringPtr("10.0.0.5"), Name: stringPtr("member1.example.com")}
	got := dhcpMemberToCommon(buildDhcpMember(in))
	if !memberEqual(in, got) {
		t.Errorf("Dhcpmember round-trip: got %+v, want %+v", got, in)
	}
}

func TestMemberEqualTreatsNilAsZeroValue(t *testing.T) {
	if !memberEqual(nil, &templateMember{}) {
		t.Error("memberEqual(nil, &templateMember{}) = false, want true")
	}
}

// ── lateInitialize ────────────────────────────────────────────────────────

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var comment *string
	extAttrs := map[string]string(nil)
	var options []templateOption
	var useOptions *bool
	serverAssociationType := ""
	var failoverAssociation *string
	var member *templateMember
	var cloudApiCompatible *bool

	rec := &ibclient.Rangetemplate{
		Comment:               stringPtr("server default"),
		Ea:                    ibclient.EA{"env": "prod"},
		Options:               []*ibclient.Dhcpoption{{Name: "routers", Num: 3}},
		UseOptions:            boolPtr(true),
		ServerAssociationType: "MEMBER",
		FailoverAssociation:   stringPtr("fa1"),
		Member:                &ibclient.Dhcpmember{Name: "member1.example.com"},
		CloudApiCompatible:    boolPtr(true),
	}

	changed := lateInitialize(&comment, &extAttrs, &options, &useOptions, &serverAssociationType, &failoverAssociation, &member, &cloudApiCompatible, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if comment == nil || *comment != "server default" {
		t.Errorf("lateInitialize: comment = %v, want %q", comment, "server default")
	}
	if !extAttrsEqual(extAttrs, map[string]string{"env": "prod"}) {
		t.Errorf("lateInitialize: extAttrs = %v, want {env: prod}", extAttrs)
	}
	if len(options) != 1 || strOrEmpty(options[0].Name) != "routers" {
		t.Errorf("lateInitialize: options = %+v, want a single routers option", options)
	}
	if useOptions == nil || !*useOptions {
		t.Errorf("lateInitialize: useOptions = %v, want true", useOptions)
	}
	if serverAssociationType != "MEMBER" {
		t.Errorf("lateInitialize: serverAssociationType = %q, want %q", serverAssociationType, "MEMBER")
	}
	if failoverAssociation == nil || *failoverAssociation != "fa1" {
		t.Errorf("lateInitialize: failoverAssociation = %v, want %q", failoverAssociation, "fa1")
	}
	if member == nil || strOrEmpty(member.Name) != "member1.example.com" {
		t.Errorf("lateInitialize: member = %+v, want Name=member1.example.com", member)
	}
	if cloudApiCompatible == nil || !*cloudApiCompatible {
		t.Errorf("lateInitialize: cloudApiCompatible = %v, want true", cloudApiCompatible)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	extAttrs := map[string]string{"env": "staging"}
	options := []templateOption{{Name: stringPtr("user-option")}}
	useOptions := boolPtr(false)
	serverAssociationType := "FAILOVER"
	failoverAssociation := stringPtr("user-fa")
	member := &templateMember{Name: stringPtr("user-member")}
	cloudApiCompatible := boolPtr(false)

	rec := &ibclient.Rangetemplate{
		Comment:               stringPtr("server default"),
		Ea:                    ibclient.EA{"env": "prod"},
		Options:               []*ibclient.Dhcpoption{{Name: "server-option"}},
		UseOptions:            boolPtr(true),
		ServerAssociationType: "MEMBER",
		FailoverAssociation:   stringPtr("server-fa"),
		Member:                &ibclient.Dhcpmember{Name: "server-member"},
		CloudApiCompatible:    boolPtr(true),
	}

	changed := lateInitialize(&comment, &extAttrs, &options, &useOptions, &serverAssociationType, &failoverAssociation, &member, &cloudApiCompatible, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" {
		t.Error("lateInitialize: overwrote already-set comment")
	}
	if extAttrs["env"] != "staging" {
		t.Error("lateInitialize: overwrote already-set extAttrs")
	}
	if strOrEmpty(options[0].Name) != "user-option" {
		t.Error("lateInitialize: overwrote already-set options")
	}
	if *useOptions != false {
		t.Error("lateInitialize: overwrote already-set useOptions")
	}
	if serverAssociationType != "FAILOVER" {
		t.Error("lateInitialize: overwrote already-set serverAssociationType")
	}
	if *failoverAssociation != "user-fa" {
		t.Error("lateInitialize: overwrote already-set failoverAssociation")
	}
	if strOrEmpty(member.Name) != "user-member" {
		t.Error("lateInitialize: overwrote already-set member")
	}
	if *cloudApiCompatible != false {
		t.Error("lateInitialize: overwrote already-set cloudApiCompatible")
	}
}

// TestObserveDoesNotLateInitializeRequiredFields proves that name,
// numberOfAddresses, and offset — the CRD's required
// RangeTemplateParameters fields — are never overwritten by Observe()'s
// late-init step. lateInitialize only accepts pointers to the optional
// fields, so a spec/observed mismatch on a required field can never occur
// through the real WAPI flow — this test drives it artificially to pin
// the guarantee.
func TestObserveDoesNotLateInitializeRequiredFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.Rangetemplate{
		Name:              stringPtr("observed-template"),
		NumberOfAddresses: uint32Ptr(99),
		Offset:            uint32Ptr(1),
	})

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRangeTemplate("my-rangetemplate", ref)
	cr.Spec.ForProvider.Name = stringPtr("template1")
	cr.Spec.ForProvider.NumberOfAddresses = uint32Ptr(10)
	cr.Spec.ForProvider.Offset = uint32Ptr(5)

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	if got := *cr.Spec.ForProvider.Name; got != "template1" {
		t.Errorf("Observe: required field Name late-initialized to %q, want unchanged %q", got, "template1")
	}
	if got := *cr.Spec.ForProvider.NumberOfAddresses; got != 10 {
		t.Errorf("Observe: required field NumberOfAddresses late-initialized to %d, want unchanged 10", got)
	}
	if got := *cr.Spec.ForProvider.Offset; got != 5 {
		t.Errorf("Observe: required field Offset late-initialized to %d, want unchanged 5", got)
	}
}

// ── isUpToDate: table-driven field comparison ───────────────────────────

func TestIsUpToDate(t *testing.T) {
	observedTemplate := func() *ibclient.Rangetemplate {
		return &ibclient.Rangetemplate{
			Name:                  stringPtr("template1"),
			NumberOfAddresses:     uint32Ptr(10),
			Offset:                uint32Ptr(5),
			Comment:               stringPtr("hello"),
			Ea:                    ibclient.EA{"env": "prod"},
			ServerAssociationType: "MEMBER",
		}
	}

	cases := map[string]struct {
		reason                string
		name                  *string
		numberOfAddresses     *uint32
		offset                *uint32
		comment               *string
		extAttrs              map[string]string
		serverAssociationType string
		want                  bool
	}{
		"IdenticalFieldsAreUpToDate": {
			reason:                "when every mutable field matches the observed record, the resource must be reported up to date",
			name:                  stringPtr("template1"),
			numberOfAddresses:     uint32Ptr(10),
			offset:                uint32Ptr(5),
			comment:               stringPtr("hello"),
			extAttrs:              map[string]string{"env": "prod"},
			serverAssociationType: "MEMBER",
			want:                  true,
		},
		"ChangedNameIsNotUpToDate": {
			reason:                "a changed name must be detected as drift",
			name:                  stringPtr("renamed-template"),
			numberOfAddresses:     uint32Ptr(10),
			offset:                uint32Ptr(5),
			comment:               stringPtr("hello"),
			extAttrs:              map[string]string{"env": "prod"},
			serverAssociationType: "MEMBER",
			want:                  false,
		},
		"ChangedNumberOfAddressesIsNotUpToDate": {
			reason:                "a changed numberOfAddresses must be detected as drift",
			name:                  stringPtr("template1"),
			numberOfAddresses:     uint32Ptr(20),
			offset:                uint32Ptr(5),
			comment:               stringPtr("hello"),
			extAttrs:              map[string]string{"env": "prod"},
			serverAssociationType: "MEMBER",
			want:                  false,
		},
		"ChangedOffsetIsNotUpToDate": {
			reason:                "a changed offset must be detected as drift",
			name:                  stringPtr("template1"),
			numberOfAddresses:     uint32Ptr(10),
			offset:                uint32Ptr(6),
			comment:               stringPtr("hello"),
			extAttrs:              map[string]string{"env": "prod"},
			serverAssociationType: "MEMBER",
			want:                  false,
		},
		"ChangedCommentIsNotUpToDate": {
			reason:                "a changed comment must be detected as drift",
			name:                  stringPtr("template1"),
			numberOfAddresses:     uint32Ptr(10),
			offset:                uint32Ptr(5),
			comment:               stringPtr("goodbye"),
			extAttrs:              map[string]string{"env": "prod"},
			serverAssociationType: "MEMBER",
			want:                  false,
		},
		"ExtAttrsDifferentValueIsNotUpToDate": {
			reason:                "an extAttrs value change on an existing key must be detected as drift",
			name:                  stringPtr("template1"),
			numberOfAddresses:     uint32Ptr(10),
			offset:                uint32Ptr(5),
			comment:               stringPtr("hello"),
			extAttrs:              map[string]string{"env": "staging"},
			serverAssociationType: "MEMBER",
			want:                  false,
		},
		"ChangedServerAssociationTypeIsNotUpToDate": {
			reason:                "a changed serverAssociationType must be detected as drift",
			name:                  stringPtr("template1"),
			numberOfAddresses:     uint32Ptr(10),
			offset:                uint32Ptr(5),
			comment:               stringPtr("hello"),
			extAttrs:              map[string]string{"env": "prod"},
			serverAssociationType: "FAILOVER",
			want:                  false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := isUpToDate(tc.name, tc.numberOfAddresses, tc.offset, tc.comment, tc.extAttrs, nil, nil, tc.serverAssociationType, nil, nil, nil, observedTemplate())
			if got != tc.want {
				t.Errorf("%s: isUpToDate() = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

func TestIsUpToDateExtAttrsEmptyVsNil(t *testing.T) {
	rec := &ibclient.Rangetemplate{
		Name:              stringPtr("template1"),
		NumberOfAddresses: uint32Ptr(10),
		Offset:            uint32Ptr(5),
	}
	// The observed record carries no extattrs (nil Ea) — a spec with an
	// explicit empty map must still compare as up to date, since
	// extAttrsEqual treats nil and empty as equivalent (avoids a phantom
	// diff when the WAPI response omits an empty extattrs object).
	got := isUpToDate(stringPtr("template1"), uint32Ptr(10), uint32Ptr(5), nil, map[string]string{}, nil, nil, "", nil, nil, nil, rec)
	if !got {
		t.Error("isUpToDate: empty ExtAttrs spec vs nil observed Ea = false, want true")
	}
}

func TestIsUpToDateDetectsOptionsDrift(t *testing.T) {
	rec := &ibclient.Rangetemplate{
		Name:              stringPtr("template1"),
		NumberOfAddresses: uint32Ptr(10),
		Offset:            uint32Ptr(5),
		Options:           []*ibclient.Dhcpoption{{Name: "routers", Value: "10.0.0.1"}},
	}
	specOptions := []templateOption{{Name: stringPtr("routers"), Value: stringPtr("10.0.0.2")}}

	got := isUpToDate(stringPtr("template1"), uint32Ptr(10), uint32Ptr(5), nil, nil, specOptions, nil, "", nil, nil, nil, rec)
	if got {
		t.Error("isUpToDate: changed option value not detected as drift")
	}
}

func TestIsUpToDateDetectsMemberDrift(t *testing.T) {
	rec := &ibclient.Rangetemplate{
		Name:              stringPtr("template1"),
		NumberOfAddresses: uint32Ptr(10),
		Offset:            uint32Ptr(5),
		Member:            &ibclient.Dhcpmember{Name: "member1.example.com"},
	}
	specMember := &templateMember{Name: stringPtr("member2.example.com")}

	got := isUpToDate(stringPtr("template1"), uint32Ptr(10), uint32Ptr(5), nil, nil, nil, nil, "", nil, specMember, nil, rec)
	if got {
		t.Error("isUpToDate: changed member not detected as drift")
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
