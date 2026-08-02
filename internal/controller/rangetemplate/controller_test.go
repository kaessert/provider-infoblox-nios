// Package rangetemplate unit tests for the RangeTemplate MR controllers.
// Tests use inline httptest.NewServer mocks that emulate the WAPI
// rangetemplate endpoints, PascalCase test names (no underscores), and
// white-box access to the unexported connectors/clients so both scopes
// can be exercised without going through the full Connect() credential
// bridge on every test.
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
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

const (
	testUIDCluster    = "test-uid-cluster"
	testUIDNamespaced = "test-uid-namespaced"
)

func stringPtr(s string) *string { return &s }
func uint32Ptr(u uint32) *uint32 { return &u }

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

func newClusterRangeTemplate(crName, externalName string) *clusterv1alpha1.RangeTemplate {
	cr := &clusterv1alpha1.RangeTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: testUIDCluster},
		Spec: clusterv1alpha1.RangeTemplateSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.RangeTemplateParameters{
				Name:              stringPtr("test-template"),
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

func newNamespacedRangeTemplate(ns, crName, externalName, pcKind string) *namespacedv1alpha1.RangeTemplate {
	cr := &namespacedv1alpha1.RangeTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: testUIDNamespaced},
		Spec: namespacedv1alpha1.RangeTemplateSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.RangeTemplateParameters{
				Name:              stringPtr("test-template"),
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

type mockWapiServer struct {
	mu          sync.Mutex
	templates   map[string]*ibclient.Rangetemplate
	nextRef     int
	searchCalls int

	eaDefExists       bool
	eaDefCreateStatus int
	eaDefCreateBody   string
	eaDefSearchCalls  int
	undefinedEASearch bool
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{
		templates:   map[string]*ibclient.Rangetemplate{},
		eaDefExists: true,
	}
}

func (m *mockWapiServer) seed(rec *ibclient.Rangetemplate) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if rec.Ref == "" {
		name := ""
		if rec.Name != nil {
			name = *rec.Name
		}
		rec.Ref = "rangetemplate/test" + itoa(m.nextRef) + ":" + name
	}
	m.templates[rec.Ref] = rec
	return rec.Ref
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
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

func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/rangetemplate", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.Rangetemplate
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ref := m.seed(&rec)
		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.eaDefSearchCalls++
		exists := m.eaDefExists
		m.mu.Unlock()
		if !exists {
			writeJSON(w, http.StatusOK, []ibclient.EADefinition{})
			return
		}
		name := identity.EAKey
		writeJSON(w, http.StatusOK, []ibclient.EADefinition{{Name: &name}})
	})

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		status := m.eaDefCreateStatus
		body := m.eaDefCreateBody
		m.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
		m.mu.Lock()
		m.eaDefExists = true
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, "extensibleattributedef/test:"+url.QueryEscape(identity.EAKey))
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/rangetemplate", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.searchCalls++
		m.mu.Unlock()

		q := r.URL.Query()
		name := q.Get("name")
		eaFilters := map[string]string{}
		for k, vals := range q {
			if strings.HasPrefix(k, "*") && len(vals) > 0 {
				eaFilters[strings.TrimPrefix(k, "*")] = vals[0]
			}
		}

		m.mu.Lock()
		undefinedEA := m.undefinedEASearch
		m.mu.Unlock()
		if len(eaFilters) > 0 && undefinedEA {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"Error":"AdmConProtoError: Unknown extensible attribute: ` + identity.EAKey + `","code":"Client.Ibap.Proto","text":"Unknown extensible attribute: ` + identity.EAKey + `"}`))
			return
		}

		m.mu.Lock()
		var matches []ibclient.Rangetemplate
		for _, rec := range m.templates {
			if name != "" && (rec.Name == nil || *rec.Name != name) {
				continue
			}
			mismatch := false
			for k, v := range eaFilters {
				got, ok := rec.Ea[k]
				if !ok {
					mismatch = true
					break
				}
				if s, ok := got.(string); !ok || s != v {
					mismatch = true
					break
				}
			}
			if mismatch {
				continue
			}
			matches = append(matches, *rec)
		}
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, matches)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		rec, ok := m.templates[ref]
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
		existing, ok := m.templates[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var incoming ibclient.Rangetemplate
		if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		// UNSTABLE _ref: renaming a range template mints a new _ref —
		// mirrors live NIOS Grid Manager behavior (the _ref encodes name).
		renamed := strOrEmpty(existing.Name) != strOrEmpty(incoming.Name)
		existing.Name = incoming.Name
		existing.NumberOfAddresses = incoming.NumberOfAddresses
		existing.Offset = incoming.Offset
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		existing.Options = incoming.Options
		existing.UseOptions = incoming.UseOptions
		respRef := ref
		if renamed {
			delete(m.templates, ref)
			m.nextRef++
			respRef = "rangetemplate/test" + itoa(m.nextRef) + ":" + strOrEmpty(existing.Name)
			existing.Ref = respRef
			m.templates[respRef] = existing
		}
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, respRef)
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

func newTestClient(t *testing.T, srv *httptest.Server) identity.ManagerAndConnector {
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
		t.Fatalf("cannot build test client: %v", err)
	}
	return mc
}

// ── Observe ──────────────────────────────────────────────────────────────

func TestClusterObserveResolvedUpToDate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rec := &ibclient.Rangetemplate{Name: stringPtr("test-template"), NumberOfAddresses: uint32Ptr(10), Offset: uint32Ptr(5)}
	rec.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(rec)

	cr := newClusterRangeTemplate("my-tpl", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists || !obs.ResourceUpToDate {
		t.Fatalf("expected exists+up-to-date, got %+v", obs)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterRangeTemplate("my-tpl", "rangetemplate/doesnotexist:my-tpl")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.ResourceExists {
		t.Fatalf("expected ResourceExists=false, got %+v", obs)
	}
}

func TestObservePreCreateState(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterRangeTemplate("my-tpl", "my-tpl")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.ResourceExists {
		t.Fatalf("expected ResourceExists=false, got %+v", obs)
	}

	m.mu.Lock()
	searchCalls := m.searchCalls
	m.mu.Unlock()
	if searchCalls == 0 {
		t.Fatal("expected the identity ladder to search by uid even in the pre-create state, got zero search calls")
	}
}

func TestClusterObserveAdoptsUnstampedObjectAndForcesUpdate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rec := &ibclient.Rangetemplate{Name: stringPtr("test-template"), NumberOfAddresses: uint32Ptr(10), Offset: uint32Ptr(5)}
	ref := m.seed(rec)

	cr := newClusterRangeTemplate("my-tpl", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("expected exists, got %+v", obs)
	}
	if obs.ResourceUpToDate {
		t.Fatal("adopted object must never report up to date")
	}
}

func TestClusterObserveRecoversRotatedRefAndPersistsAnnotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rec := &ibclient.Rangetemplate{Name: stringPtr("test-template"), NumberOfAddresses: uint32Ptr(10), Offset: uint32Ptr(5)}
	rec.Ea = identity.Stamp(nil, testUIDCluster)
	realRef := m.seed(rec)

	cr := newClusterRangeTemplate("my-tpl", "rangetemplate/stale:my-tpl")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists {
		t.Fatalf("expected exists, got %+v", obs)
	}
	if meta.GetExternalName(cr) != realRef {
		t.Fatalf("expected external-name refreshed to %q, got %q", realRef, meta.GetExternalName(cr))
	}
}

func TestClusterObserveRefusesOnForeignIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rec := &ibclient.Rangetemplate{Name: stringPtr("test-template")}
	rec.Ea = identity.Stamp(nil, "someone-elses-uid")
	ref := m.seed(rec)

	cr := newClusterRangeTemplate("my-tpl", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("expected an error for foreign identity")
	}
}

// ── Create ───────────────────────────────────────────────────────────────

func TestClusterCreateStampsIdentity(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterRangeTemplate("my-tpl", "my-tpl")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.GetExternalName(cr) == "my-tpl" {
		t.Fatal("expected external-name to be set to the server _ref")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rec := range m.templates {
		got, ok := rec.Ea[identity.EAKey]
		if !ok || got != testUIDCluster {
			t.Fatalf("expected identity stamp %q, got %v", testUIDCluster, rec.Ea)
		}
	}
}

func TestCreateRangeTemplateRefusesEmptyUID(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	if _, err := createRangeTemplate(mc.Manager, stringPtr("x"), uint32Ptr(10), uint32Ptr(5), nil, nil, nil, nil, "", nil, nil, nil, nil, ""); err == nil {
		t.Fatal("expected an error for empty uid")
	}
}

// ── Update ───────────────────────────────────────────────────────────────

func TestClusterUpdateReassertsIdentityStamp(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rec := &ibclient.Rangetemplate{Name: stringPtr("test-template"), NumberOfAddresses: uint32Ptr(10), Offset: uint32Ptr(5)}
	rec.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(rec)

	cr := newClusterRangeTemplate("my-tpl", ref)
	cr.Spec.ForProvider.Comment = stringPtr("updated")
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(cr).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	updated := m.templates[ref]
	if updated.Ea[identity.EAKey] != testUIDCluster {
		t.Fatalf("expected identity stamp to survive update, got %v", updated.Ea)
	}
}

// ── Delete ───────────────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rec := &ibclient.Rangetemplate{Name: stringPtr("test-template")}
	rec.Ea = identity.Stamp(nil, testUIDCluster)
	ref := m.seed(rec)

	cr := newClusterRangeTemplate("my-tpl", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.templates[ref]; ok {
		t.Fatal("expected the object to be deleted")
	}
}

func TestClusterDeleteNotFoundIsSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	cr := newClusterRangeTemplate("my-tpl", "rangetemplate/gone:my-tpl")
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("expected nil error for already-gone object, got %v", err)
	}
}

func TestClusterDeleteRefusesUnverifiedOwnership(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	mc := newTestClient(t, srv)

	rec := &ibclient.Rangetemplate{Name: stringPtr("test-template")}
	ref := m.seed(rec)

	cr := newClusterRangeTemplate("my-tpl", ref)
	e := &clusterExternal{objMgr: mc.Manager, conn: mc.Connector}

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("expected delete to be refused for an unstamped object")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.templates[ref]; !ok {
		t.Fatal("object must not be deleted when ownership cannot be verified")
	}
}

// ── Connect ──────────────────────────────────────────────────────────────

func TestClusterConnectProviderConfigNotFound(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &clusterConnector{kube: kube, usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{})}
	cr := newClusterRangeTemplate("my-tpl", "")

	if _, err := c.Connect(context.Background(), cr); err == nil {
		t.Fatal("expected an error when ProviderConfig is missing")
	}
}

func TestClusterConnectSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	scheme := newTestScheme(t)
	pc := &clusterpcv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: clusterpcv1alpha1.ProviderConfigSpec{
			Credentials: clusterpcv1alpha1.ProviderCredentials{
				Source: xpv1.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
					SecretRef: &xpv1.SecretKeySelector{Key: "creds", SecretReference: xpv1.SecretReference{Name: "creds", Namespace: "ns"}},
				},
			},
		},
	}
	secret := credentialsSecret("ns", "creds", u.Hostname(), "user", "pass")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pc, secret).Build()
	c := &clusterConnector{kube: kube, usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{})}
	cr := newClusterRangeTemplate("my-tpl", "")

	if _, err := c.Connect(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNamespacedConnectUnsupportedKind(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	c := &namespacedConnector{kube: kube, usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{})}
	cr := newNamespacedRangeTemplate("ns", "my-tpl", "", "SomethingElse")

	if _, err := c.Connect(context.Background(), cr); err == nil {
		t.Fatal("expected an error for an unsupported providerConfigRef Kind")
	}
}

func TestNamespacedConnectWithClusterProviderConfig(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	scheme := newTestScheme(t)
	cpc := &namespacedpcv1alpha1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: namespacedpcv1alpha1.ProviderConfigSpec{
			Credentials: namespacedpcv1alpha1.ProviderCredentials{
				Source: xpv1.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
					SecretRef: &xpv1.SecretKeySelector{Key: "creds", SecretReference: xpv1.SecretReference{Name: "creds", Namespace: "ns"}},
				},
			},
		},
	}
	secret := credentialsSecret("ns", "creds", u.Hostname(), "user", "pass")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cpc, secret).Build()
	c := &namespacedConnector{kube: kube, usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{})}
	cr := newNamespacedRangeTemplate("ns", "my-tpl", "", "ClusterProviderConfig")

	if _, err := c.Connect(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ── newEmpty correctness (dual-object-type gate — single-type resource) ──

func TestNewEmptyRangeTemplateCorrectness(t *testing.T) {
	rec := ibclient.NewEmptyRangeTemplate()
	if rec.ObjectType() != "rangetemplate" {
		t.Fatalf("expected ObjectType rangetemplate, got %q", rec.ObjectType())
	}
	found := false
	for _, f := range rec.ReturnFields() {
		if f == "extattrs" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected ReturnFields to include extattrs, got %v", rec.ReturnFields())
	}
}

// ── Identity EA must never late-init into spec.forProvider ───────────────

func TestLateInitializeDoesNotLeakIdentityKeyIntoExtAttrs(t *testing.T) {
	rec := &ibclient.Rangetemplate{Name: stringPtr("test-template")}
	rec.Ea = identity.Stamp(ibclient.EA{"Site": "dc1"}, "some-uid")

	var comment *string
	var extAttrs map[string]string
	options := []templateOption{}
	var useOptions *bool
	var serverAssociationType string
	var failoverAssociation *string
	var member *templateMember
	var cloudApiCompatible *bool
	lateInitialize(&comment, &extAttrs, &options, &useOptions, &serverAssociationType, &failoverAssociation, &member, &cloudApiCompatible, rec)

	if _, ok := extAttrs[identity.EAKey]; ok {
		t.Fatalf("identity key must never late-init into spec.forProvider.extAttrs, got %v", extAttrs)
	}
	if extAttrs["Site"] != "dc1" {
		t.Fatalf("expected non-reserved EA to still be back-filled, got %v", extAttrs)
	}
}

func TestIsUpToDateIgnoresIdentityEA(t *testing.T) {
	rec := &ibclient.Rangetemplate{Name: stringPtr("test-template"), NumberOfAddresses: uint32Ptr(10), Offset: uint32Ptr(5)}
	rec.Ea = identity.Stamp(nil, "some-uid")

	if !isUpToDate(stringPtr("test-template"), uint32Ptr(10), uint32Ptr(5), nil, nil, nil, nil, "", nil, nil, nil, rec) {
		t.Fatal("expected isUpToDate to ignore the identity EA when spec.extAttrs is empty")
	}
}

func TestClusterDisconnectIsNoop(t *testing.T) {
	e := &clusterExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNamespacedDisconnectIsNoop(t *testing.T) {
	e := &namespacedExternal{}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
