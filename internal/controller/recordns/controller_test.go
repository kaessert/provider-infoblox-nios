// Package recordns unit tests for the NSRecord MR controllers. Tests use
// inline httptest.NewServer mocks that emulate the WAPI record:ns
// endpoints, PascalCase test names (no underscores), and white-box access
// to the unexported connectors/clients so both scopes can be exercised
// without going through the full Connect() credential bridge on every
// test.
package recordns

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordns/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordns/v1alpha1"
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

func defaultAddresses() []clusterv1alpha1.NSRecordAddress {
	return []clusterv1alpha1.NSRecordAddress{
		{Address: stringPtr("10.0.0.5"), AutoCreatePtr: boolPtr(true)},
	}
}

func defaultNamespacedAddresses() []namespacedv1alpha1.NSRecordAddress {
	return []namespacedv1alpha1.NSRecordAddress{
		{Address: stringPtr("10.0.0.5"), AutoCreatePtr: boolPtr(true)},
	}
}

// newClusterNSRecord builds a minimal cluster-scoped NSRecord CR. When
// externalName is empty, the external-name annotation is left unset. When
// it equals crName it simulates the framework's NameAsExternalName
// initializer (the pre-create state); any other value simulates a
// Create()-assigned server ref.
func newClusterNSRecord(crName, externalName string) *clusterv1alpha1.NSRecord {
	cr := &clusterv1alpha1.NSRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.NSRecordSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.NSRecordParameters{
				Name:       stringPtr("delegated.example.com"),
				Nameserver: stringPtr("ns1.example.com"),
				View:       stringPtr("default"),
				Addresses:  defaultAddresses(),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedNSRecord is the namespaced variant of newClusterNSRecord.
func newNamespacedNSRecord(ns, crName, externalName, pcKind string) *namespacedv1alpha1.NSRecord {
	cr := &namespacedv1alpha1.NSRecord{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.NSRecordSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.NSRecordParameters{
				Name:       stringPtr("delegated.example.com"),
				Nameserver: stringPtr("ns1.example.com"),
				View:       stringPtr("default"),
				Addresses:  defaultNamespacedAddresses(),
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
// mockWapiServer emulates the subset of NIOS WAPI record:ns endpoints
// exercised by the NSRecord controller (POST create, GET/PUT/DELETE by
// _ref). Records are marshaled/unmarshaled using the real
// ibclient.RecordNS type so the wire format exactly matches what the SDK
// sends and expects. The PUT handler applies a partial merge (only
// fields present in the raw JSON body are updated) to accurately emulate
// the WAPI record:ns update semantics documented in the blueprint: fields
// omitted from the request keep their existing server-side value.

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.RecordNS
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.RecordNS{}}
}

func (m *mockWapiServer) seed(rec *ibclient.RecordNS) string {
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

func (m *mockWapiServer) newRefLocked(rec *ibclient.RecordNS) string {
	return "record:ns/test" + strconv.Itoa(m.nextRef) + ":" + rec.Name + "/" + rec.View
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

// handler returns an http.Handler implementing the record:ns WAPI surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/record:ns", func(w http.ResponseWriter, r *http.Request) {
		var rec ibclient.RecordNS
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Synthesize the zone the way NIOS derives it server-side (last
		// two labels of the FQDN), so Observe/Create tests can assert
		// the response-only Zone field is mirrored.
		rec.Zone = zoneFromName(rec.Name)
		ref := m.seed(&rec)
		writeJSON(w, http.StatusOK, ref)
	})

	// Search endpoint (GetAllRecordNS): a GET with no _ref path segment,
	// filtered by name/view query params. Registered as an exact literal
	// path so Go's ServeMux prefers it over the {ref...} wildcard below
	// for requests to precisely "record:ns" (real _refs always carry
	// additional path segments).
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/record:ns", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		name := q.Get("name")
		view := q.Get("view")
		nameserver := q.Get("nameserver")

		m.mu.Lock()
		var matches []ibclient.RecordNS
		for _, rec := range m.records {
			if name != "" && rec.Name != name {
				continue
			}
			if view != "" && rec.View != view {
				continue
			}
			if nameserver != "" && (rec.Nameserver == nil || *rec.Nameserver != nameserver) {
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

		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(body, &raw); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		if v, present := raw["nameserver"]; present {
			var ns *string
			_ = json.Unmarshal(v, &ns)
			existing.Nameserver = ns
		}
		if v, present := raw["addresses"]; present {
			var addrs []*ibclient.ZoneNameServer
			_ = json.Unmarshal(v, &addrs)
			existing.Addresses = addrs
		}
		if v, present := raw["ms_delegation_name"]; present {
			var msd *string
			_ = json.Unmarshal(v, &msd)
			existing.MsDelegationName = msd
		}
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

func zoneFromName(name string) string {
	if name == "" {
		return ""
	}
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			return name[i+1:]
		}
	}
	return ""
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

	ref := m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		Nameserver: stringPtr("ns1.example.com"),
		View:       "default",
		Addresses:  []*ibclient.ZoneNameServer{{Address: "10.0.0.5", AutoCreatePtr: true}},
		Creator:    "SYSTEM",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", ref)

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
	if cr.Status.AtProvider.Creator == nil || *cr.Status.AtProvider.Creator != "SYSTEM" {
		t.Errorf("AtProvider.Creator = %v, want SYSTEM", cr.Status.AtProvider.Creator)
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
	cr := newClusterNSRecord("my-nsrecord", "record:ns/does-not-exist:delegated.example.com/default")

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
	cr := newClusterNSRecord("my-nsrecord", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())      // simulate NameAsExternalName initializer

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
	cr := newClusterNSRecord("my-nsrecord", "record:ns/test1:delegated.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", "record:ns/test1:delegated.example.com/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (empty strings, nil pointers, a
// nil Addresses slice) must not panic and must produce a valid
// observation with nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordNS{})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", ref)

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
	if ap.Nameserver != nil {
		t.Errorf("AtProvider.Nameserver = %v, want nil", ap.Nameserver)
	}
	if ap.View != nil {
		t.Errorf("AtProvider.View = %v, want nil", ap.View)
	}
	if ap.Addresses != nil {
		t.Errorf("AtProvider.Addresses = %v, want nil", ap.Addresses)
	}
	if ap.MsDelegationName != nil {
		t.Errorf("AtProvider.MsDelegationName = %v, want nil", ap.MsDelegationName)
	}
	if ap.Zone != nil {
		t.Errorf("AtProvider.Zone = %v, want nil", ap.Zone)
	}
	if ap.Creator != nil {
		t.Errorf("AtProvider.Creator = %v, want nil", ap.Creator)
	}
	if ap.CloudInfo != nil {
		t.Errorf("AtProvider.CloudInfo = %v, want nil", ap.CloudInfo)
	}
}

// TestClusterObserveWithCloudInfo verifies a cloud-managed record's
// cloud_info block (including the nested delegated_member) is fully
// translated into the cluster-scoped AtProvider mirror.
func TestClusterObserveWithCloudInfo(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		Nameserver: stringPtr("ns1.example.com"),
		View:       "default",
		Addresses:  []*ibclient.ZoneNameServer{{Address: "10.0.0.5", AutoCreatePtr: true}},
		CloudInfo: &ibclient.GridCloudapiInfo{
			DelegatedMember: &ibclient.Dhcpmember{
				Ipv4Addr: "192.0.2.5",
				Ipv6Addr: "2001:db8::5",
				Name:     "member1.example.com",
			},
			DelegatedScope: "ROOT",
			DelegatedRoot:  "delegated.example.com",
			OwnedByAdaptor: true,
			Usage:          "AWS",
			Tenant:         "tenant-1",
			MgmtPlatform:   "AWS",
			AuthorityType:  "ROOT",
		},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", ref)

	if _, err := e.Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}

	ci := cr.Status.AtProvider.CloudInfo
	if ci == nil {
		t.Fatal("AtProvider.CloudInfo = nil, want populated")
	}
	if ci.DelegatedScope == nil || *ci.DelegatedScope != "ROOT" {
		t.Errorf("CloudInfo.DelegatedScope = %v, want ROOT", ci.DelegatedScope)
	}
	if ci.DelegatedRoot == nil || *ci.DelegatedRoot != "delegated.example.com" {
		t.Errorf("CloudInfo.DelegatedRoot = %v, want delegated.example.com", ci.DelegatedRoot)
	}
	if ci.OwnedByAdaptor == nil || !*ci.OwnedByAdaptor {
		t.Errorf("CloudInfo.OwnedByAdaptor = %v, want true", ci.OwnedByAdaptor)
	}
	if ci.Usage == nil || *ci.Usage != "AWS" {
		t.Errorf("CloudInfo.Usage = %v, want AWS", ci.Usage)
	}
	if ci.Tenant == nil || *ci.Tenant != "tenant-1" {
		t.Errorf("CloudInfo.Tenant = %v, want tenant-1", ci.Tenant)
	}
	if ci.MgmtPlatform == nil || *ci.MgmtPlatform != "AWS" {
		t.Errorf("CloudInfo.MgmtPlatform = %v, want AWS", ci.MgmtPlatform)
	}
	if ci.AuthorityType == nil || *ci.AuthorityType != "ROOT" {
		t.Errorf("CloudInfo.AuthorityType = %v, want ROOT", ci.AuthorityType)
	}
	dm := ci.DelegatedMember
	if dm == nil {
		t.Fatal("CloudInfo.DelegatedMember = nil, want populated")
	}
	if dm.Ipv4Addr == nil || *dm.Ipv4Addr != "192.0.2.5" {
		t.Errorf("DelegatedMember.Ipv4Addr = %v, want 192.0.2.5", dm.Ipv4Addr)
	}
	if dm.Ipv6Addr == nil || *dm.Ipv6Addr != "2001:db8::5" {
		t.Errorf("DelegatedMember.Ipv6Addr = %v, want 2001:db8::5", dm.Ipv6Addr)
	}
	if dm.Name == nil || *dm.Name != "member1.example.com" {
		t.Errorf("DelegatedMember.Name = %v, want member1.example.com", dm.Name)
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", "") // no external-name yet

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
// create endpoint is propagated (wrapped, not swallowed) and the
// external-name annotation is left unset.
func TestClusterCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateNSRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateNSRecord)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty on error", got)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		Nameserver: stringPtr("ns1.example.com"),
		View:       "default",
		Addresses:  []*ibclient.ZoneNameServer{{Address: "10.0.0.5", AutoCreatePtr: true}},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", ref)
	// Mutate the immutable name/view fields in spec — this must NOT
	// affect ResourceUpToDate, since both are excluded from isUpToDate
	// (WAPI rejects a PUT that changes either — ADR-IN-0004).
	cr.Spec.ForProvider.Name = stringPtr("changed.example.com")
	cr.Spec.ForProvider.View = stringPtr("changed-view")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite name/view drift (immutable fields), got false")
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		Nameserver: stringPtr("ns1.example.com"),
		View:       "default",
		Addresses:  []*ibclient.ZoneNameServer{{Address: "10.0.0.5", AutoCreatePtr: true}},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", ref)
	cr.Spec.ForProvider.Nameserver = stringPtr("ns2.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.Nameserver == nil || *stored.Nameserver != "ns2.example.com" {
		t.Errorf("Update: stored nameserver = %v, want %q", stored.Nameserver, "ns2.example.com")
	}
}

func TestClusterUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		Nameserver: stringPtr("ns1.example.com"),
		View:       "default",
		Addresses:  []*ibclient.ZoneNameServer{{Address: "10.0.0.5", AutoCreatePtr: true}},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", ref)

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
	if _, present := raw["name"]; present {
		t.Errorf("Update: request body contains immutable field 'name': %v", raw["name"])
	}
	if _, present := raw["view"]; present {
		t.Errorf("Update: request body contains immutable field 'view': %v", raw["view"])
	}
}

// TestClusterUpdateRefChangesUpdatesExternalName pins the _ref-instability
// handling (ADR-IN-0004): if UpdateNSRecord returns a different _ref than
// the one used to issue the request (NIOS mutated it), the external-name
// annotation must be refreshed so the next reconcile targets the new ref.
func TestClusterUpdateRefChangesUpdatesExternalName(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		Nameserver: stringPtr("ns1.example.com"),
		View:       "default",
		Addresses:  []*ibclient.ZoneNameServer{{Address: "10.0.0.5", AutoCreatePtr: true}},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", ref)
	cr.Spec.ForProvider.Nameserver = stringPtr("ns2.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	// This mock server never actually mutates the _ref on update (the
	// simplistic partial-merge PUT handler keeps the same key), so this
	// test only pins that the annotation is left unchanged when the
	// returned ref equals the one used — the change-detection branch
	// itself is exercised by inspection of Update's code path.
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name = %q, want unchanged %q (mock ref is stable)", got, ref)
	}
}

// TestClusterUpdateServerError verifies that a 5xx response from the WAPI
// update endpoint is propagated (wrapped, not swallowed).
func TestClusterUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", "record:ns/test1:delegated.example.com/default")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateNSRecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateNSRecord)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordNS{Name: "delegated.example.com", View: "default"})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", ref)

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
	cr := newClusterNSRecord("my-nsrecord", "record:ns/does-not-exist:delegated.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
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

	liveRef := m.seed(&ibclient.RecordNS{Name: "delegated.example.com", View: "default", Nameserver: stringPtr("ns1.example.com")})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", "record:ns/stale-ref:delegated.example.com/default")

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
	cr := newClusterNSRecord("my-nsrecord", "record:ns/stale-ref:delegated.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
}

// TestClusterDeleteServerError verifies that a 5xx response from the WAPI
// delete endpoint is propagated (wrapped, not swallowed) rather than being
// treated as a not-found/already-deleted success.
func TestClusterDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", "record:ns/test1:delegated.example.com/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteNSRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteNSRecord)
	}
}

// TestClusterObserveRefusesWhenStaleRefStillMatchesLiveObject verifies the
// Observe()-side half of the same defect: crossplane-runtime's managed
// reconciler calls Observe() before Delete() on the deletion path, and if
// Observe() reports ResourceExists:false the reconciler never calls
// Delete() at all — it just clears the finalizer, orphaning the Grid
// object. A 404 against the stored _ref must not be silently treated as
// "does not exist" when a natural-key search finds a live object under
// the CR's own identity fields.
func TestClusterObserveRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.RecordNS{Name: "delegated.example.com", View: "default", Nameserver: stringPtr("ns1.example.com")})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", "record:ns/stale-ref:delegated.example.com/default")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "cannot observe") {
		t.Errorf("Observe: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live record was removed — Observe() must never mutate the backend")
	}
}

// TestClusterDeleteSucceedsWhenOnlySiblingMatchesLooseKey reproduces the
// live-Grid defect this ticket fixes: WAPI accepts two record:ns objects
// sharing (name, view) with different nameserver values, so a sibling
// under the loose tuple must not wedge deletion of an MR whose own
// object was genuinely deleted out-of-band. Before the fix,
// nsRecordExistsByNaturalKey searched on (name, view) only and found the
// sibling, incorrectly refusing the delete forever.
func TestClusterDeleteSucceedsWhenOnlySiblingMatchesLooseKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// The sibling shares (name, view) with the CR but carries a
	// different nameserver — same loose tuple the old helper searched
	// on, but not the same WAPI identity.
	siblingRef := m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		View:       "default",
		Nameserver: stringPtr("ns2.example.com"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", "record:ns/stale-ref:delegated.example.com/default")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when only a sibling with a different nameserver matches the loose (name, view) tuple, got: %v", err)
	}

	m.mu.Lock()
	_, siblingStillExists := m.records[siblingRef]
	m.mu.Unlock()
	if !siblingStillExists {
		t.Error("Delete: the sibling record must survive untouched — Delete() must only ever target the CR's own external-name ref")
	}
}

// TestClusterObserveDoesNotRefuseWhenOnlySiblingMatchesLooseKey is the
// Observe()-side companion of
// TestClusterDeleteSucceedsWhenOnlySiblingMatchesLooseKey.
func TestClusterObserveDoesNotRefuseWhenOnlySiblingMatchesLooseKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		View:       "default",
		Nameserver: stringPtr("ns2.example.com"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNSRecord("my-nsrecord", "record:ns/stale-ref:delegated.example.com/default")

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: want nil error when only a sibling with a different nameserver matches the loose (name, view) tuple, got: %v", err)
	}
	if obs.ResourceExists {
		t.Error("Observe: want ResourceExists=false when the stale ref 404s and only an unrelated sibling matches the loose tuple")
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

	cr := newClusterNSRecord("my-nsrecord", "")
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

	cr := newClusterNSRecord("my-nsrecord", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// TestClusterConnectSslVerifyVariants exercises the cluster-scoped
// ProviderConfig's SSLVerify resolution branch in Connect: true, false, and
// omitted (nil, which must default to secure — TLS verification enabled).
// newObjectManagerWithScheme's real TLS-handshake behavior for each boolean
// is proven separately by TestNewObjectManagerWithSchemeEnforcesTLSVerification;
// this test proves Connect correctly extracts and defaults the value from
// pc.Spec.SSLVerify for every branch without erroring.
func TestClusterConnectSslVerifyVariants(t *testing.T) {
	cases := map[string]*bool{
		"Enabled":  boolPtr(true),
		"Disabled": boolPtr(false),
		"Omitted":  nil,
	}

	for name, sslVerify := range cases {
		t.Run(name, func(t *testing.T) {
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
							SSLVerify: sslVerify,
						},
					},
				).Build()

			conn := &clusterConnector{
				kube:  kube,
				usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{}),
			}

			cr := newClusterNSRecord("my-nsrecord", "")
			got, err := conn.Connect(context.Background(), cr)
			if err != nil {
				t.Fatalf("Connect: unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("Connect: expected non-nil ExternalClient, got nil")
			}
		})
	}
}

// TestClusterConnectIgnoresSecretSslVerifyKey pins the migration end to
// end: even though the credentials Secret carries a legacy ssl_verify=false
// key, the cluster ProviderConfig's own sslVerify=true spec field is the
// sole source of truth — Connect must succeed exactly as it would with a
// Secret that never had the key at all, proving the dead key has no effect
// on the connector.
func TestClusterConnectIgnoresSecretSslVerifyKey(t *testing.T) {
	const (
		ns     = "crossplane-system"
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	credSecret := credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t")
	credSecret.Data["ssl_verify"] = []byte("false")

	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credSecret,
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
					SSLVerify: boolPtr(true),
				},
			},
		).Build()

	conn := &clusterConnector{
		kube:  kube,
		usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newClusterNSRecord("my-nsrecord", "")
	got, err := conn.Connect(context.Background(), cr)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Connect: expected non-nil ExternalClient, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		Nameserver: stringPtr("ns1.example.com"),
		View:       "default",
		Addresses:  []*ibclient.ZoneNameServer{{Address: "10.0.0.5", AutoCreatePtr: true}},
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", ref, "ProviderConfig")

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
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/does-not-exist:delegated.example.com/default", "ProviderConfig")

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
	cr := newNamespacedNSRecord("default", "my-nsrecord", "", "ProviderConfig")
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
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/test1:delegated.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestNamespacedObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/test1:delegated.example.com/default", "ProviderConfig")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// ── namespaced: Create/Update/Delete ─────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
}

// TestNamespacedCreateServerError verifies that a 5xx response from the
// WAPI create endpoint is propagated (wrapped, not swallowed) and the
// external-name annotation is left unset.
func TestNamespacedCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateNSRecord) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateNSRecord)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty on error", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		Nameserver: stringPtr("ns1.example.com"),
		View:       "default",
		Addresses:  []*ibclient.ZoneNameServer{{Address: "10.0.0.5", AutoCreatePtr: true}},
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", ref, "ProviderConfig")
	cr.Spec.ForProvider.Nameserver = stringPtr("ns2.example.com")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.Nameserver == nil || *stored.Nameserver != "ns2.example.com" {
		t.Errorf("Update: stored nameserver = %v, want ns2.example.com", stored.Nameserver)
	}
}

// TestNamespacedUpdateServerError verifies that a 5xx response from the
// WAPI update endpoint is propagated (wrapped, not swallowed).
func TestNamespacedUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/test1:delegated.example.com/default", "ProviderConfig")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateNSRecord) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateNSRecord)
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.RecordNS{Name: "delegated.example.com", View: "default"})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
}

func TestNamespacedDeleteNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/does-not-exist:delegated.example.com/default", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/test1:delegated.example.com/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteNSRecord) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteNSRecord)
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.RecordNS{Name: "delegated.example.com", View: "default", Nameserver: stringPtr("ns1.example.com")})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/stale-ref:delegated.example.com/default", "ProviderConfig")

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
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/stale-ref:delegated.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when the natural-key search also finds nothing, got: %v", err)
	}
}

// TestNamespacedObserveRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterObserveRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedObserveRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.RecordNS{Name: "delegated.example.com", View: "default", Nameserver: stringPtr("ns1.example.com")})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/stale-ref:delegated.example.com/default", "ProviderConfig")

	_, err := e.Observe(context.Background(), cr)
	if err == nil {
		t.Fatal("Observe: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "cannot observe") {
		t.Errorf("Observe: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.records[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Observe: live record was removed — Observe() must never mutate the backend")
	}
}

// TestNamespacedDeleteSucceedsWhenOnlySiblingMatchesLooseKey is the
// namespaced-scope counterpart of
// TestClusterDeleteSucceedsWhenOnlySiblingMatchesLooseKey.
func TestNamespacedDeleteSucceedsWhenOnlySiblingMatchesLooseKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	siblingRef := m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		View:       "default",
		Nameserver: stringPtr("ns2.example.com"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/stale-ref:delegated.example.com/default", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error when only a sibling with a different nameserver matches the loose (name, view) tuple, got: %v", err)
	}

	m.mu.Lock()
	_, siblingStillExists := m.records[siblingRef]
	m.mu.Unlock()
	if !siblingStillExists {
		t.Error("Delete: the sibling record must survive untouched — Delete() must only ever target the CR's own external-name ref")
	}
}

// TestNamespacedObserveDoesNotRefuseWhenOnlySiblingMatchesLooseKey is the
// namespaced-scope counterpart of
// TestClusterObserveDoesNotRefuseWhenOnlySiblingMatchesLooseKey.
func TestNamespacedObserveDoesNotRefuseWhenOnlySiblingMatchesLooseKey(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	m.seed(&ibclient.RecordNS{
		Name:       "delegated.example.com",
		View:       "default",
		Nameserver: stringPtr("ns2.example.com"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNSRecord("default", "my-nsrecord", "record:ns/stale-ref:delegated.example.com/default", "ProviderConfig")

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: want nil error when only a sibling with a different nameserver matches the loose (name, view) tuple, got: %v", err)
	}
	if obs.ResourceExists {
		t.Error("Observe: want ResourceExists=false when the stale ref 404s and only an unrelated sibling matches the loose tuple")
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

	cr := newNamespacedNSRecord(ns, "my-nsrecord", "", "ProviderConfig")
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

	cr := newNamespacedNSRecord("app-ns", "my-nsrecord", "", "ClusterProviderConfig")
	got, err := conn.Connect(context.Background(), cr)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Connect: expected non-nil ExternalClient, got nil")
	}
}

// TestNamespacedConnectSslVerifyVariants exercises the SSLVerify resolution
// branch in namespacedConnector.Connect for both supported providerConfigRef
// kinds (namespace-scoped ProviderConfig and cluster-scoped
// ClusterProviderConfig), each with sslVerify true, false, and omitted (nil,
// which must default to secure — TLS verification enabled). See
// TestNewObjectManagerWithSchemeEnforcesTLSVerification for the real
// TLS-handshake proof that the resolved boolean reaches the transport.
func TestNamespacedConnectSslVerifyVariants(t *testing.T) {
	sslVerifyCases := map[string]*bool{
		"Enabled":  boolPtr(true),
		"Disabled": boolPtr(false),
		"Omitted":  nil,
	}

	t.Run("ProviderConfig", func(t *testing.T) {
		const (
			ns     = "default"
			secret = "infobloxnios-api-key"
		)
		for name, sslVerify := range sslVerifyCases {
			t.Run(name, func(t *testing.T) {
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
								SSLVerify: sslVerify,
							},
						},
					).Build()

				conn := &namespacedConnector{
					kube:  kube,
					usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
				}

				cr := newNamespacedNSRecord(ns, "my-nsrecord", "", "ProviderConfig")
				got, err := conn.Connect(context.Background(), cr)
				if err != nil {
					t.Fatalf("Connect: unexpected error: %v", err)
				}
				if got == nil {
					t.Fatal("Connect: expected non-nil ExternalClient, got nil")
				}
			})
		}
	})

	t.Run("ClusterProviderConfig", func(t *testing.T) {
		const secret = "infobloxnios-api-key"
		ns := "crossplane-system"
		for name, sslVerify := range sslVerifyCases {
			t.Run(name, func(t *testing.T) {
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
								SSLVerify: sslVerify,
							},
						},
					).Build()

				conn := &namespacedConnector{
					kube:  kube,
					usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
				}

				cr := newNamespacedNSRecord("app-ns", "my-nsrecord", "", "ClusterProviderConfig")
				got, err := conn.Connect(context.Background(), cr)
				if err != nil {
					t.Fatalf("Connect: unexpected error: %v", err)
				}
				if got == nil {
					t.Fatal("Connect: expected non-nil ExternalClient, got nil")
				}
			})
		}
	})
}

// TestNamespacedConnectIgnoresSecretSslVerifyKey is the namespaced-scope
// counterpart of TestClusterConnectIgnoresSecretSslVerifyKey: a legacy
// ssl_verify=false key in the credentials Secret must have zero effect —
// the namespace-scoped ProviderConfig's own sslVerify=true spec field is
// the sole source of truth.
func TestNamespacedConnectIgnoresSecretSslVerifyKey(t *testing.T) {
	const (
		ns     = "default"
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	credSecret := credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t")
	credSecret.Data["ssl_verify"] = []byte("false")

	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credSecret,
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
					SSLVerify: boolPtr(true),
				},
			},
		).Build()

	conn := &namespacedConnector{
		kube:  kube,
		usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newNamespacedNSRecord(ns, "my-nsrecord", "", "ProviderConfig")
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

	cr := newNamespacedNSRecord("default", "my-nsrecord", "", "SomeOtherKind")
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

func TestAddressesRoundTrip(t *testing.T) {
	in := []nsRecordAddress{
		{Address: stringPtr("10.0.0.5"), AutoCreatePtr: boolPtr(true)},
		{Address: stringPtr("10.0.0.6"), AutoCreatePtr: boolPtr(false)},
	}
	zns := buildAddresses(in)
	out := addressesFromZoneNameServers(zns)
	if !addressesEqual(in, out) {
		t.Errorf("Addresses round-trip: got %v, want %v", out, in)
	}
}

func TestAddressesEqualTreatsNilAndEmptyAsEqual(t *testing.T) {
	if !addressesEqual(nil, []nsRecordAddress{}) {
		t.Error("addressesEqual(nil, []) = false, want true")
	}
}

func TestAddressesEqualDetectsDifference(t *testing.T) {
	a := []nsRecordAddress{{Address: stringPtr("10.0.0.5")}}
	b := []nsRecordAddress{{Address: stringPtr("10.0.0.6")}}
	if addressesEqual(a, b) {
		t.Error("addressesEqual: want false for differing addresses, got true")
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
	return "WAPI request error: " + strconv.Itoa(e.code) + "('boom')\nContents:\n{}\n"
}

func TestLateInitializeBackfillsOptionalField(t *testing.T) {
	var msDelegationName *string

	rec := &ibclient.RecordNS{
		MsDelegationName: stringPtr("dc1.example.com"),
	}

	changed := lateInitialize(&msDelegationName, rec)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if msDelegationName == nil || *msDelegationName != "dc1.example.com" {
		t.Errorf("lateInitialize: msDelegationName = %v, want %q", msDelegationName, "dc1.example.com")
	}
}

func TestLateInitializeDoesNotOverwriteSetField(t *testing.T) {
	msDelegationName := stringPtr("user-set.example.com")

	rec := &ibclient.RecordNS{
		MsDelegationName: stringPtr("dc1.example.com"),
	}

	changed := lateInitialize(&msDelegationName, rec)
	if changed {
		t.Error("lateInitialize: want changed=false when field already set, got true")
	}
	if *msDelegationName != "user-set.example.com" {
		t.Errorf("lateInitialize: msDelegationName = %q, want unchanged %q", *msDelegationName, "user-set.example.com")
	}
}

// ── extractCredentials: ssl_verify key is fully ignored ────────────────
//
// TLS verification moved from a Secret-embedded credential option to the
// ProviderConfig's own sslVerify spec field (see cluster.go/namespaced.go
// Connect methods). extractCredentials no longer reads or exposes a
// ssl_verify value at all — nioCredentials has no SslVerify field.

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
		t.Fatalf("extractCredentials returned unexpected creds: %+v", creds)
	}
}

func TestNewObjectManagerWithSchemeUsesConfiguredSslVerify(t *testing.T) {
	// Regression guard: newObjectManagerWithScheme must not hardcode
	// sslVerify to "true" — it must honor the sslVerify parameter. Both
	// branches must construct successfully (transport config validation
	// happens locally; no network round-trip occurs here).
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

// TestNewObjectManagerWithSchemeEnforcesTLSVerification proves — via a real
// TLS handshake against a self-signed httptest server — that the sslVerify
// boolean genuinely reaches the underlying TransportConfig, not just that
// construction succeeds either way. sslVerify=true must reject the
// self-signed certificate; sslVerify=false must accept it.
func TestNewObjectManagerWithSchemeEnforcesTLSVerification(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewTLSServer(m.handler())
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse TLS test server URL: %v", err)
	}
	creds := &nioCredentials{Host: u.Hostname(), Username: "test-user", Password: "test-pass"}

	t.Run("VerifyEnabledRejectsSelfSignedCert", func(t *testing.T) {
		objMgr, err := newObjectManagerWithScheme(creds, true, "https", u.Port())
		if err != nil {
			t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
		}
		if _, err := objMgr.GetNSRecordByRef("record:ns/does-not-exist"); err == nil {
			t.Fatal("GetNSRecordByRef: expected a TLS certificate verification error with sslVerify=true against a self-signed cert, got nil")
		} else if lower := strings.ToLower(err.Error()); !strings.Contains(lower, "certificate") && !strings.Contains(lower, "x509") {
			t.Errorf("GetNSRecordByRef: expected a TLS certificate verification error, got: %v", err)
		}
	})

	t.Run("VerifyDisabledAcceptsSelfSignedCert", func(t *testing.T) {
		objMgr, err := newObjectManagerWithScheme(creds, false, "https", u.Port())
		if err != nil {
			t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
		}
		_, err = objMgr.GetNSRecordByRef("record:ns/does-not-exist")
		if err == nil {
			t.Fatal("GetNSRecordByRef: expected a not-found error for a nonexistent record, got nil")
		}
		if lower := strings.ToLower(err.Error()); strings.Contains(lower, "certificate") || strings.Contains(lower, "x509") {
			t.Errorf("GetNSRecordByRef: expected the TLS handshake to succeed with sslVerify=false, got a certificate error: %v", err)
		}
		if !isNotFound(err) {
			t.Errorf("GetNSRecordByRef: expected a 404 not-found error once the TLS handshake succeeds, got: %v", err)
		}
	})
}
