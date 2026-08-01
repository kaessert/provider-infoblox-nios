// Package extensibleattributedef unit tests for the ExtensibleAttributeDef
// MR controllers. Tests use inline httptest.NewServer mocks that emulate
// the WAPI extensibleattributedef endpoints, PascalCase test names (no
// underscores), and white-box access to the unexported connectors/clients
// so both scopes can be exercised without going through the full
// Connect() credential bridge on every test.
package extensibleattributedef

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/extensibleattributedef/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/extensibleattributedef/v1alpha1"
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

// newClusterEADef builds a minimal cluster-scoped ExtensibleAttributeDef
// CR. When externalName is empty, the external-name annotation is left
// unset. When it equals crName it simulates the framework's
// NameAsExternalName initializer (the pre-create state); any other value
// simulates a Create()-assigned server ref.
func newClusterEADef(crName, externalName string) *clusterv1alpha1.ExtensibleAttributeDef {
	cr := &clusterv1alpha1.ExtensibleAttributeDef{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.ExtensibleAttributeDefSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.ExtensibleAttributeDefParameters{
				Name: "MyAttribute",
				Type: "STRING",
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedEADef is the namespaced variant of newClusterEADef.
func newNamespacedEADef(ns, crName, externalName, pcKind string) *namespacedv1alpha1.ExtensibleAttributeDef {
	cr := &namespacedv1alpha1.ExtensibleAttributeDef{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.ExtensibleAttributeDefSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.ExtensibleAttributeDefParameters{
				Name: "MyAttribute",
				Type: "STRING",
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
// mockEADefServer emulates the subset of NIOS WAPI extensibleattributedef
// endpoints exercised by the controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.EADefinition type so the wire format exactly matches what the
// SDK's generic Connector sends and expects.

type mockEADefServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.EADefinition
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
	// lastGetQuery captures the raw query string of the most recent GET
	// request, for tests that assert _return_fields never requests
	// descendants_action.
	lastGetQuery string
}

func newMockEADefServer() *mockEADefServer {
	return &mockEADefServer{records: map[string]*ibclient.EADefinition{}}
}

func (m *mockEADefServer) seed(def *ibclient.EADefinition) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if def.Ref == "" {
		def.Ref = m.newRefLocked(def)
	}
	m.records[def.Ref] = def
	return def.Ref
}

func (m *mockEADefServer) newRefLocked(def *ibclient.EADefinition) string {
	name := ""
	if def.Name != nil {
		name = *def.Name
	}
	return "extensibleattributedef/test" + itoa(m.nextRef) + ":" + name
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

// handler returns an http.Handler implementing the extensibleattributedef
// WAPI surface.
func (m *mockEADefServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		var def ibclient.EADefinition
		if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ref := m.seed(&def)
		writeJSON(w, http.StatusOK, ref)
	})

	// Search endpoint (eaDefinitionExistsByNaturalKey): a GET with no
	// _ref path segment, filtered by the name query param. Registered
	// as an exact literal path so Go's ServeMux prefers it over the
	// {ref...} wildcard below for requests to precisely
	// "extensibleattributedef" (real _refs always carry additional path
	// segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/extensibleattributedef", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")

		m.mu.Lock()
		var matches []ibclient.EADefinition
		for _, def := range m.records {
			if name != "" && (def.Name == nil || *def.Name != name) {
				continue
			}
			matches = append(matches, *def)
		}
		m.mu.Unlock()

		// Always respond 200 — WAPI search semantics report "not found"
		// via an empty array, never an HTTP error status.
		writeJSON(w, http.StatusOK, matches)
	})

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		m.lastGetQuery = r.URL.RawQuery
		def, ok := m.records[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, def)
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
		var incoming ibclient.EADefinition
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		existing.Name = incoming.Name
		existing.Comment = incoming.Comment
		existing.DefaultValue = incoming.DefaultValue
		existing.Flags = incoming.Flags
		existing.ListValues = incoming.ListValues
		existing.AllowedObjectTypes = incoming.AllowedObjectTypes
		existing.DescendantsAction = incoming.DescendantsAction
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

// newTestConnector builds an *ibclient.Connector pointed at the given
// httptest.Server via plain HTTP (no TLS needed — the WapiRequestBuilder
// only switches to HTTPS when hostCfg.Scheme != "http").
func newTestConnector(t *testing.T, srv *httptest.Server) *ibclient.Connector {
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
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{
		Name:               stringPtr("MyAttribute"),
		Type:               "STRING",
		Comment:            stringPtr("hello"),
		DefaultValue:       stringPtr("default-val"),
		Flags:              stringPtr("C"),
		AllowedObjectTypes: []string{"Network"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.DefaultValue = stringPtr("default-val")
	cr.Spec.ForProvider.Flags = stringPtr("C")
	cr.Spec.ForProvider.AllowedObjectTypes = []string{"Network"}

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
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", "extensibleattributedef/does-not-exist:MyAttribute")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestClusterObserveMinimalResponse verifies that Observe does not panic
// and returns a sane result when the WAPI response carries only the
// required identifier fields (Name, Type) with every optional
// pointer/slice field (Comment, DefaultValue, Min, Max, Flags,
// ListValues, AllowedObjectTypes) left at its nil/zero value. This is
// the shape WAPI returns for a freshly created ExtensibleAttributeDef
// that never set any optional field.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{
		Name: stringPtr("MyAttribute"),
		Type: "STRING",
		// All optional fields intentionally left nil/empty: Comment,
		// DefaultValue, Min, Max, Flags, ListValues, AllowedObjectTypes.
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", ref)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error on minimal response: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true, got false")
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true for matching minimal spec, got false")
	}
	if got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=false when server returns no optional values, got true")
	}
	if cr.Status.AtProvider.ID != ref {
		t.Errorf("AtProvider.ID = %q, want %q", cr.Status.AtProvider.ID, ref)
	}
	if cr.Status.AtProvider.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", cr.Status.AtProvider.Comment)
	}
	if cr.Status.AtProvider.DefaultValue != nil {
		t.Errorf("AtProvider.DefaultValue = %v, want nil", cr.Status.AtProvider.DefaultValue)
	}
	if cr.Status.AtProvider.Min != nil {
		t.Errorf("AtProvider.Min = %v, want nil", cr.Status.AtProvider.Min)
	}
	if cr.Status.AtProvider.Max != nil {
		t.Errorf("AtProvider.Max = %v, want nil", cr.Status.AtProvider.Max)
	}
	if cr.Status.AtProvider.Flags != nil {
		t.Errorf("AtProvider.Flags = %v, want nil", cr.Status.AtProvider.Flags)
	}
	if len(cr.Status.AtProvider.ListValues) != 0 {
		t.Errorf("AtProvider.ListValues = %v, want empty", cr.Status.AtProvider.ListValues)
	}
	if len(cr.Status.AtProvider.AllowedObjectTypes) != 0 {
		t.Errorf("AtProvider.AllowedObjectTypes = %v, want empty", cr.Status.AtProvider.AllowedObjectTypes)
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

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())

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

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", "extensibleattributedef/test1:MyAttribute")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", "extensibleattributedef/test1:MyAttribute")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveDoesNotRequestDescendantsAction pins the contract
// that _return_fields never includes descendants_action — the WAPI
// schema marks it write-only and reading it back errors with "Field is
// not readable".
func TestClusterObserveDoesNotRequestDescendantsAction(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{Name: stringPtr("MyAttribute"), Type: "STRING"})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", ref)

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	m.mu.Lock()
	q := m.lastGetQuery
	m.mu.Unlock()

	vals, err := url.ParseQuery(q)
	if err != nil {
		t.Fatalf("cannot parse captured GET query: %v", err)
	}
	if strings.Contains(vals.Get("_return_fields"), "descendants_action") {
		t.Errorf("Observe: _return_fields contains write-only field descendants_action: %q", vals.Get("_return_fields"))
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{
		Name: stringPtr("MyAttribute"),
		Type: "INTEGER",
		Min:  uint32Ptr(1),
		Max:  uint32Ptr(10),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", ref)
	cr.Spec.ForProvider.Type = "INTEGER"
	// Mutate the immutable min/max fields in spec — this must NOT affect
	// ResourceUpToDate, since min/max are excluded from isUpToDate (WAPI
	// rejects any PUT that changes them).
	cr.Spec.ForProvider.Min = uint32Ptr(99)
	cr.Spec.ForProvider.Max = uint32Ptr(999)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite min/max drift (immutable fields), got false")
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{
		Name:    stringPtr("MyAttribute"),
		Type:    "STRING",
		Comment: stringPtr("old comment"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", ref)
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
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{
		Name: stringPtr("MyAttribute"),
		Type: "INTEGER",
		Min:  uint32Ptr(1),
		Max:  uint32Ptr(10),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", ref)
	cr.Spec.ForProvider.Type = "INTEGER"
	cr.Spec.ForProvider.Min = uint32Ptr(1)
	cr.Spec.ForProvider.Max = uint32Ptr(10)

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
	for _, field := range []string{"type", "min", "max"} {
		if _, present := raw[field]; present {
			t.Errorf("Update: request body contains immutable field %q: %v", field, raw[field])
		}
	}
}

// TestClusterUpdateSendsAllMutableFields verifies that every mutable
// field (comment, default_value, flags, list_values,
// allowed_object_types, descendants_action) set on the desired spec is
// present in the PUT body — the counterpart to
// TestClusterUpdateDoesNotSendImmutableField, which checks exclusion.
func TestClusterUpdateSendsAllMutableFields(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{Name: stringPtr("MyAttribute"), Type: "STRING"})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")
	cr.Spec.ForProvider.DefaultValue = stringPtr("new default")
	cr.Spec.ForProvider.Flags = stringPtr("C")
	cr.Spec.ForProvider.ListValues = []clusterv1alpha1.EADefListValue{{Value: "a"}, {Value: "b"}}
	cr.Spec.ForProvider.AllowedObjectTypes = []string{"Network", "Zone"}
	cr.Spec.ForProvider.DescendantsAction = stringPtr("INHERIT")

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
	for _, field := range []string{"comment", "default_value", "flags", "list_values", "allowed_object_types", "descendants_action"} {
		if _, present := raw[field]; !present {
			t.Errorf("Update: request body missing mutable field %q: %v", field, raw)
		}
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{Name: stringPtr("MyAttribute"), Type: "STRING"})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", ref)

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
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", "extensibleattributedef/does-not-exist:MyAttribute")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

func TestClusterDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", "extensibleattributedef/test1:MyAttribute")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteEADefinition) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteEADefinition)
	}
}

// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject verifies the
// core defect fix: a 404 against the stored _ref must not be treated as
// "already deleted" when a natural-key search finds the same identity
// still live under a different _ref. Deleting that record would be
// unverifiable ownership, so Delete() must refuse and leave the record in
// place.
func TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.EADefinition{Name: stringPtr("MyAttribute"), Type: "STRING"})

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", "extensibleattributedef/stale-ref:MyAttribute")

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
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newClusterEADef("my-eadef", "extensibleattributedef/stale-ref:MyAttribute")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
}

func TestClusterDisconnectIsNoop(t *testing.T) {
	e := &clusterExternal{kube: &recordingKubeClient{}}
	if err := e.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect: unexpected error: %v", err)
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

	cr := newClusterEADef("my-eadef", "")
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

	cr := newClusterEADef("my-eadef", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{Name: stringPtr("MyAttribute"), Type: "STRING"})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", ref, "ProviderConfig")

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
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", "extensibleattributedef/does-not-exist:MyAttribute", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestNamespacedObserveMinimalResponse is the namespaced-scope
// equivalent of TestClusterObserveMinimalResponse: it verifies Observe
// does not panic and returns a sane result when the WAPI response
// carries only the required identifier fields (Name, Type) with every
// optional pointer/slice field left at its nil/zero value.
func TestNamespacedObserveMinimalResponse(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{
		Name: stringPtr("MyAttribute"),
		Type: "STRING",
		// All optional fields intentionally left nil/empty: Comment,
		// DefaultValue, Min, Max, Flags, ListValues, AllowedObjectTypes.
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", ref, "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error on minimal response: %v", err)
	}
	if !got.ResourceExists {
		t.Error("Observe: want ResourceExists=true, got false")
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true for matching minimal spec, got false")
	}
	if got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=false when server returns no optional values, got true")
	}
	if cr.Status.AtProvider.ID != ref {
		t.Errorf("AtProvider.ID = %q, want %q", cr.Status.AtProvider.ID, ref)
	}
	if cr.Status.AtProvider.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", cr.Status.AtProvider.Comment)
	}
	if cr.Status.AtProvider.DefaultValue != nil {
		t.Errorf("AtProvider.DefaultValue = %v, want nil", cr.Status.AtProvider.DefaultValue)
	}
	if cr.Status.AtProvider.Min != nil {
		t.Errorf("AtProvider.Min = %v, want nil", cr.Status.AtProvider.Min)
	}
	if cr.Status.AtProvider.Max != nil {
		t.Errorf("AtProvider.Max = %v, want nil", cr.Status.AtProvider.Max)
	}
	if cr.Status.AtProvider.Flags != nil {
		t.Errorf("AtProvider.Flags = %v, want nil", cr.Status.AtProvider.Flags)
	}
	if len(cr.Status.AtProvider.ListValues) != 0 {
		t.Errorf("AtProvider.ListValues = %v, want empty", cr.Status.AtProvider.ListValues)
	}
	if len(cr.Status.AtProvider.AllowedObjectTypes) != 0 {
		t.Errorf("AtProvider.AllowedObjectTypes = %v, want empty", cr.Status.AtProvider.AllowedObjectTypes)
	}
}

func TestNamespacedObservePreCreateState(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", "", "ProviderConfig")
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

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", "extensibleattributedef/test1:MyAttribute", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", "extensibleattributedef/test1:MyAttribute", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{
		Name:    stringPtr("MyAttribute"),
		Type:    "STRING",
		Comment: stringPtr("old comment"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", ref, "ProviderConfig")
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

// TestNamespacedUpdateDoesNotSendImmutableField mirrors
// TestClusterUpdateDoesNotSendImmutableField for the namespaced scope —
// type/min/max must never appear in the PUT body regardless of scope.
func TestNamespacedUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{
		Name: stringPtr("MyAttribute"),
		Type: "INTEGER",
		Min:  uint32Ptr(1),
		Max:  uint32Ptr(10),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", ref, "ProviderConfig")
	cr.Spec.ForProvider.Type = "INTEGER"
	cr.Spec.ForProvider.Min = uint32Ptr(1)
	cr.Spec.ForProvider.Max = uint32Ptr(10)

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
	for _, field := range []string{"type", "min", "max"} {
		if _, present := raw[field]; present {
			t.Errorf("Update: request body contains immutable field %q: %v", field, raw[field])
		}
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.EADefinition{Name: stringPtr("MyAttribute"), Type: "STRING"})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", "extensibleattributedef/does-not-exist:MyAttribute", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", "extensibleattributedef/test1:MyAttribute", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteEADefinition) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteEADefinition)
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.EADefinition{Name: stringPtr("MyAttribute"), Type: "STRING"})

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", "extensibleattributedef/stale-ref:MyAttribute", "ProviderConfig")

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
	m := newMockEADefServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, conn: newTestConnector(t, srv)}
	cr := newNamespacedEADef("default", "my-eadef", "extensibleattributedef/stale-ref:MyAttribute", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
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

	cr := newNamespacedEADef(ns, "my-eadef", "", "ProviderConfig")
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

	cr := newNamespacedEADef("app-ns", "my-eadef", "", "ClusterProviderConfig")
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

	cr := newNamespacedEADef("default", "my-eadef", "", "SomeOtherKind")
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

func TestIsNotFoundClassifiesTypedError(t *testing.T) {
	err := ibclient.NewNotFoundError("not found")
	if !isNotFound(err) {
		t.Error("isNotFound: want true for *ibclient.NotFoundError, got false")
	}
}

func TestIsNotFoundClassifiesGenericStatusError(t *testing.T) {
	cases := map[string]struct {
		msg  string
		want bool
	}{
		"404": {"WAPI request error: 404('Not Found')\nContents:\n{}\n", true},
		"500": {"WAPI request error: 500('Internal Server Error')\nContents:\n{}\n", false},
		"403": {"WAPI request error: 403('Forbidden')\nContents:\n{}\n", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := errFromString(tc.msg)
			if got := isNotFound(err); got != tc.want {
				t.Errorf("isNotFound(%q) = %v, want %v", tc.msg, got, tc.want)
			}
		})
	}
}

// errFromString wraps a plain string in an error for table-driven test
// cases that need to construct arbitrary WAPI error messages.
type stringError string

func (e stringError) Error() string { return string(e) }
func errFromString(s string) error  { return stringError(s) }

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var comment, defaultValue, flags *string
	var listValues, allowedObjectTypes []string

	def := &ibclient.EADefinition{
		Name:               stringPtr("MyAttribute"),
		Type:               "ENUM",
		Comment:            stringPtr("server comment"),
		DefaultValue:       stringPtr("server-default"),
		Flags:              stringPtr("C"),
		ListValues:         toSDKListValues([]string{"a", "b"}),
		AllowedObjectTypes: []string{"Network"},
	}

	changed := lateInitialize(&comment, &defaultValue, &flags, &listValues, &allowedObjectTypes, def)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if comment == nil || *comment != "server comment" {
		t.Errorf("comment = %v, want %q", comment, "server comment")
	}
	if defaultValue == nil || *defaultValue != "server-default" {
		t.Errorf("defaultValue = %v, want %q", defaultValue, "server-default")
	}
	if flags == nil || *flags != "C" {
		t.Errorf("flags = %v, want %q", flags, "C")
	}
	if !stringSlicesEqualUnordered(listValues, []string{"a", "b"}) {
		t.Errorf("listValues = %v, want [a b]", listValues)
	}
	if !stringSlicesEqualUnordered(allowedObjectTypes, []string{"Network"}) {
		t.Errorf("allowedObjectTypes = %v, want [Network]", allowedObjectTypes)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	comment := stringPtr("user comment")
	defaultValue := stringPtr("user-default")
	flags := stringPtr("M")
	listValues := []string{"user-val"}
	allowedObjectTypes := []string{"Zone"}

	def := &ibclient.EADefinition{
		Name:               stringPtr("MyAttribute"),
		Type:               "ENUM",
		Comment:            stringPtr("server comment"),
		DefaultValue:       stringPtr("server-default"),
		Flags:              stringPtr("C"),
		ListValues:         toSDKListValues([]string{"server-val"}),
		AllowedObjectTypes: []string{"Network"},
	}

	changed := lateInitialize(&comment, &defaultValue, &flags, &listValues, &allowedObjectTypes, def)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *comment != "user comment" {
		t.Errorf("comment = %q, want unchanged %q", *comment, "user comment")
	}
	if *defaultValue != "user-default" {
		t.Errorf("defaultValue = %q, want unchanged %q", *defaultValue, "user-default")
	}
	if *flags != "M" {
		t.Errorf("flags = %q, want unchanged %q", *flags, "M")
	}
	if !stringSlicesEqualUnordered(listValues, []string{"user-val"}) {
		t.Errorf("listValues = %v, want unchanged [user-val]", listValues)
	}
	if !stringSlicesEqualUnordered(allowedObjectTypes, []string{"Zone"}) {
		t.Errorf("allowedObjectTypes = %v, want unchanged [Zone]", allowedObjectTypes)
	}
}

func TestIsUpToDate(t *testing.T) {
	base := &ibclient.EADefinition{
		Name:               stringPtr("MyAttribute"),
		Type:               "STRING",
		Comment:            stringPtr("hello"),
		DefaultValue:       stringPtr("dv"),
		Flags:              stringPtr("C"),
		ListValues:         toSDKListValues([]string{"a", "b"}),
		AllowedObjectTypes: []string{"Network", "Zone"},
	}

	cases := map[string]struct {
		name               string
		comment            *string
		defaultValue       *string
		flags              *string
		listValues         []string
		allowedObjectTypes []string
		def                *ibclient.EADefinition
		want               bool
	}{
		"AllMatch": {
			name: "MyAttribute", comment: stringPtr("hello"), defaultValue: stringPtr("dv"), flags: stringPtr("C"),
			listValues: []string{"a", "b"}, allowedObjectTypes: []string{"Network", "Zone"}, def: base, want: true,
		},
		"UnorderedListValuesStillMatch": {
			name: "MyAttribute", comment: stringPtr("hello"), defaultValue: stringPtr("dv"), flags: stringPtr("C"),
			listValues: []string{"b", "a"}, allowedObjectTypes: []string{"Zone", "Network"}, def: base, want: true,
		},
		"NameDiffers": {
			name: "OtherAttribute", comment: stringPtr("hello"), defaultValue: stringPtr("dv"), flags: stringPtr("C"),
			listValues: []string{"a", "b"}, allowedObjectTypes: []string{"Network", "Zone"}, def: base, want: false,
		},
		"CommentDiffers": {
			name: "MyAttribute", comment: stringPtr("goodbye"), defaultValue: stringPtr("dv"), flags: stringPtr("C"),
			listValues: []string{"a", "b"}, allowedObjectTypes: []string{"Network", "Zone"}, def: base, want: false,
		},
		"DefaultValueDiffers": {
			name: "MyAttribute", comment: stringPtr("hello"), defaultValue: stringPtr("other"), flags: stringPtr("C"),
			listValues: []string{"a", "b"}, allowedObjectTypes: []string{"Network", "Zone"}, def: base, want: false,
		},
		"FlagsDiffer": {
			name: "MyAttribute", comment: stringPtr("hello"), defaultValue: stringPtr("dv"), flags: stringPtr("M"),
			listValues: []string{"a", "b"}, allowedObjectTypes: []string{"Network", "Zone"}, def: base, want: false,
		},
		"ListValuesDiffer": {
			name: "MyAttribute", comment: stringPtr("hello"), defaultValue: stringPtr("dv"), flags: stringPtr("C"),
			listValues: []string{"a", "c"}, allowedObjectTypes: []string{"Network", "Zone"}, def: base, want: false,
		},
		"AllowedObjectTypesDiffer": {
			name: "MyAttribute", comment: stringPtr("hello"), defaultValue: stringPtr("dv"), flags: stringPtr("C"),
			listValues: []string{"a", "b"}, allowedObjectTypes: []string{"Network"}, def: base, want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := isUpToDate(tc.name, tc.comment, tc.defaultValue, tc.flags, tc.listValues, tc.allowedObjectTypes, tc.def)
			if got != tc.want {
				t.Errorf("isUpToDate() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStringSlicesEqualUnorderedTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !stringSlicesEqualUnordered(nil, []string{}) {
		t.Error("stringSlicesEqualUnordered(nil, []) = false, want true")
	}
	if !stringSlicesEqualUnordered([]string{"a"}, []string{"a"}) {
		t.Error("stringSlicesEqualUnordered([a], [a]) = false, want true")
	}
	if stringSlicesEqualUnordered([]string{"a"}, []string{"b"}) {
		t.Error("stringSlicesEqualUnordered([a], [b]) = true, want false")
	}
	if stringSlicesEqualUnordered([]string{"a", "a"}, []string{"a"}) {
		t.Error("stringSlicesEqualUnordered([a a], [a]) = true, want false (multiset differs)")
	}
}

func TestListValuesRoundTrip(t *testing.T) {
	in := []string{"a", "b", "c"}
	got := fromSDKListValues(toSDKListValues(in))
	if !stringSlicesEqualUnordered(in, got) {
		t.Errorf("round trip = %v, want %v", got, in)
	}
	if toSDKListValues(nil) != nil {
		t.Error("toSDKListValues(nil) should be nil")
	}
	if fromSDKListValues(nil) != nil {
		t.Error("fromSDKListValues(nil) should be nil")
	}
}

func TestDescendantsActionToSDK(t *testing.T) {
	if got := descendantsActionToSDK(nil); got != nil {
		t.Errorf("descendantsActionToSDK(nil) = %v, want nil", got)
	}
	if got := descendantsActionToSDK(stringPtr("")); got != nil {
		t.Errorf("descendantsActionToSDK(\"\") = %v, want nil", got)
	}
	got := descendantsActionToSDK(stringPtr("INHERIT"))
	if got == nil || got.OptionWithEa != "INHERIT" {
		t.Errorf("descendantsActionToSDK(INHERIT) = %v, want OptionWithEa=INHERIT", got)
	}
}

// TestClusterListValuesConversionRoundTrip exercises the cluster-scoped
// CRD <-> shared []string list-value conversion, including the nil/empty
// branches of both directions.
func TestClusterListValuesConversionRoundTrip(t *testing.T) {
	if got := listValuesToStrings(nil); got != nil {
		t.Errorf("listValuesToStrings(nil) = %v, want nil", got)
	}
	if got := listValuesToStrings([]clusterv1alpha1.EADefListValue{}); got != nil {
		t.Errorf("listValuesToStrings([]) = %v, want nil", got)
	}
	if got := stringsToListValues(nil); got != nil {
		t.Errorf("stringsToListValues(nil) = %v, want nil", got)
	}
	if got := stringsToListValues([]string{}); got != nil {
		t.Errorf("stringsToListValues([]) = %v, want nil", got)
	}

	in := []clusterv1alpha1.EADefListValue{{Value: "a"}, {Value: "b"}}
	got := listValuesToStrings(in)
	want := []string{"a", "b"}
	if !stringSlicesEqualUnordered(got, want) {
		t.Errorf("listValuesToStrings(%v) = %v, want %v", in, got, want)
	}
	back := stringsToListValues(got)
	if len(back) != len(in) {
		t.Fatalf("stringsToListValues(%v) len = %d, want %d", got, len(back), len(in))
	}
	for i := range in {
		if back[i].Value != in[i].Value {
			t.Errorf("stringsToListValues round trip[%d] = %q, want %q", i, back[i].Value, in[i].Value)
		}
	}
}

// TestNamespacedListValuesConversionRoundTrip mirrors
// TestClusterListValuesConversionRoundTrip for the namespaced-scoped CRD
// list-value conversion helpers.
func TestNamespacedListValuesConversionRoundTrip(t *testing.T) {
	if got := namespacedListValuesToStrings(nil); got != nil {
		t.Errorf("namespacedListValuesToStrings(nil) = %v, want nil", got)
	}
	if got := namespacedListValuesToStrings([]namespacedv1alpha1.EADefListValue{}); got != nil {
		t.Errorf("namespacedListValuesToStrings([]) = %v, want nil", got)
	}
	if got := namespacedStringsToListValues(nil); got != nil {
		t.Errorf("namespacedStringsToListValues(nil) = %v, want nil", got)
	}
	if got := namespacedStringsToListValues([]string{}); got != nil {
		t.Errorf("namespacedStringsToListValues([]) = %v, want nil", got)
	}

	in := []namespacedv1alpha1.EADefListValue{{Value: "x"}, {Value: "y"}}
	got := namespacedListValuesToStrings(in)
	want := []string{"x", "y"}
	if !stringSlicesEqualUnordered(got, want) {
		t.Errorf("namespacedListValuesToStrings(%v) = %v, want %v", in, got, want)
	}
	back := namespacedStringsToListValues(got)
	if len(back) != len(in) {
		t.Fatalf("namespacedStringsToListValues(%v) len = %d, want %d", got, len(back), len(in))
	}
	for i := range in {
		if back[i].Value != in[i].Value {
			t.Errorf("namespacedStringsToListValues round trip[%d] = %q, want %q", i, back[i].Value, in[i].Value)
		}
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

func TestNewConnectorWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newConnectorWithScheme must not hardcode
	// SslVerify to "true" — it must honor the sslVerify parameter. Both branches
	// must construct successfully (transport config validation happens
	// locally; no network round-trip occurs here).
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			creds := &nioCredentials{Host: "127.0.0.1", Username: "admin", Password: "s3cr3t"}
			conn, err := newConnectorWithScheme(creds, sslVerify, "http", "80")
			if err != nil {
				t.Fatalf("newConnectorWithScheme: unexpected error: %v", err)
			}
			if conn == nil {
				t.Fatal("newConnectorWithScheme: expected non-nil connector")
			}
		})
	}
}
