// Package fixedaddress unit tests for the FixedAddress MR controllers.
// Tests use inline httptest.NewServer mocks that emulate the WAPI
// fixedaddress/ipv6fixedaddress endpoints, PascalCase test names (no
// underscores), and white-box access to the unexported connectors/clients
// so both scopes can be exercised without going through the full
// Connect() credential bridge on every test.
package fixedaddress

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/fixedaddress/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/fixedaddress/v1alpha1"
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
func int64Ptr(i int64) *int64    { return &i }
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

// newClusterFixedAddress builds a minimal cluster-scoped IPv4 FixedAddress
// CR. When externalName is empty, the external-name annotation is left
// unset. When it equals crName it simulates the framework's
// NameAsExternalName initializer (the pre-create state); any other value
// simulates a Create()-assigned server ref.
func newClusterFixedAddress(crName, externalName string) *clusterv1alpha1.FixedAddress {
	cr := &clusterv1alpha1.FixedAddress{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.FixedAddressSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.FixedAddressParameters{
				IPv4Addr:    stringPtr("10.0.0.5"),
				MAC:         stringPtr("00:11:22:33:44:55"),
				NetworkView: stringPtr("default"),
				MatchClient: stringPtr("MAC_ADDRESS"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newClusterFixedAddressIPv6 is the IPv6 variant of newClusterFixedAddress.
func newClusterFixedAddressIPv6(crName, externalName string) *clusterv1alpha1.FixedAddress {
	cr := &clusterv1alpha1.FixedAddress{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster-v6"},
		Spec: clusterv1alpha1.FixedAddressSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: "default"},
			},
			ForProvider: clusterv1alpha1.FixedAddressParameters{
				IPv6Addr:    stringPtr("2001:db8::5"),
				MAC:         stringPtr("00:11:22:33:44:66"), // carries the DUID for IPv6
				NetworkView: stringPtr("default"),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedFixedAddress is the namespaced variant of
// newClusterFixedAddress.
func newNamespacedFixedAddress(ns, crName, externalName, pcKind string) *namespacedv1alpha1.FixedAddress {
	cr := &namespacedv1alpha1.FixedAddress{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.FixedAddressSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: "default"},
			},
			ForProvider: namespacedv1alpha1.FixedAddressParameters{
				IPv4Addr:    stringPtr("10.0.0.5"),
				MAC:         stringPtr("00:11:22:33:44:55"),
				NetworkView: stringPtr("default"),
				MatchClient: stringPtr("MAC_ADDRESS"),
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
// mockWapiServer emulates the subset of NIOS WAPI fixedaddress /
// ipv6fixedaddress endpoints exercised by the FixedAddress controller
// (POST create via AllocateIP, GET/PUT/DELETE by _ref). Records are
// marshaled/unmarshaled using the real ibclient.FixedAddress type so the
// wire format (including the EA {"value": ...} envelope) exactly matches
// what the SDK sends and expects. PUT mints a new _ref whenever the
// address field (ipv4addr/ipv6addr) changes, mirroring the UNSTABLE _ref
// behavior documented for this resource (ADR-IN-0004).

type mockWapiServer struct {
	mu      sync.Mutex
	records map[string]*ibclient.FixedAddress
	nextRef int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert on outbound field values.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{records: map[string]*ibclient.FixedAddress{}}
}

func (m *mockWapiServer) seed(objectType string, fa *ibclient.FixedAddress) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if fa.Ref == "" {
		fa.Ref = m.newRefLocked(objectType, fa)
	}
	m.records[fa.Ref] = fa
	return fa.Ref
}

func (m *mockWapiServer) newRefLocked(objectType string, fa *ibclient.FixedAddress) string {
	addr := fa.IPv4Address
	if objectType == "ipv6fixedaddress" {
		addr = fa.IPv6Address
	}
	return objectType + "/test" + itoa(m.nextRef) + ":" + addr
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

// handler returns an http.Handler implementing the fixedaddress /
// ipv6fixedaddress WAPI surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	create := func(objectType string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var fa ibclient.FixedAddress
			if err := json.NewDecoder(r.Body).Decode(&fa); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			ref := m.seed(objectType, &fa)
			writeJSON(w, http.StatusOK, ref)
		}
	}
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/fixedaddress", create("fixedaddress"))
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/ipv6fixedaddress", create("ipv6fixedaddress"))

	// Search endpoint (GetFixedAddress): a GET with no _ref path segment,
	// filtered by network_view/network/ipv4addr(+mac) or
	// ipv6addr(+duid) query params depending on address family.
	// Registered as an exact literal path so Go's ServeMux prefers it
	// over the {ref...} wildcard below for requests to precisely
	// "fixedaddress"/"ipv6fixedaddress" (real _refs always carry
	// additional path segments).
	search := func(objectType string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			networkView := q.Get("network_view")
			network := q.Get("network")

			m.mu.Lock()
			var matches []ibclient.FixedAddress
			for ref, fa := range m.records {
				isIPv6 := strings.HasPrefix(ref, "ipv6fixedaddress/")
				if isIPv6 && objectType != "ipv6fixedaddress" {
					continue
				}
				if !isIPv6 && objectType != "fixedaddress" {
					continue
				}
				if networkView != "" && fa.NetviewName != networkView {
					continue
				}
				if network != "" && fa.Cidr != network {
					continue
				}
				if objectType == "ipv6fixedaddress" {
					if ipv6addr := q.Get("ipv6addr"); ipv6addr != "" && fa.IPv6Address != ipv6addr {
						continue
					}
					if duid := q.Get("duid"); duid != "" && fa.Duid != duid {
						continue
					}
				} else {
					if ipv4addr := q.Get("ipv4addr"); ipv4addr != "" && fa.IPv4Address != ipv4addr {
						continue
					}
					if mac := q.Get("mac"); mac != "" && (fa.Mac == nil || *fa.Mac != mac) {
						continue
					}
				}
				matches = append(matches, *fa)
			}
			m.mu.Unlock()

			// Always respond 200 — WAPI search semantics report
			// "not found" via an empty array, never an HTTP error
			// status.
			writeJSON(w, http.StatusOK, matches)
		}
	}
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/fixedaddress", search("fixedaddress"))
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/ipv6fixedaddress", search("ipv6fixedaddress"))

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		fa, ok := m.records[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, fa)
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
		var incoming ibclient.FixedAddress
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		objectType := "fixedaddress"
		isIPv6 := strings.HasPrefix(ref, "ipv6fixedaddress/")
		if isIPv6 {
			objectType = "ipv6fixedaddress"
		}

		m.mu.Lock()
		m.lastUpdateBody = body

		// UNSTABLE _ref: changing the address family's literal value
		// mints a new _ref — mirrors live NIOS Grid Manager behavior
		// (ADR-IN-0004).
		refMutated := existing.IPv4Address != incoming.IPv4Address || existing.IPv6Address != incoming.IPv6Address

		existing.NetviewName = incoming.NetviewName
		existing.Cidr = incoming.Cidr
		existing.IPv4Address = incoming.IPv4Address
		existing.IPv6Address = incoming.IPv6Address
		existing.Mac = incoming.Mac
		existing.Name = incoming.Name
		existing.MatchClient = incoming.MatchClient
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		existing.Disable = incoming.Disable
		existing.AgentCircuitId = incoming.AgentCircuitId
		existing.AgentRemoteId = incoming.AgentRemoteId
		existing.ClientIdentifierPrependZero = incoming.ClientIdentifierPrependZero
		existing.DhcpClientIdentifier = incoming.DhcpClientIdentifier
		existing.Options = incoming.Options
		existing.UseOptions = incoming.UseOptions

		newRef := ref
		if refMutated {
			m.nextRef++
			newRef = m.newRefLocked(objectType, existing)
			delete(m.records, ref)
			existing.Ref = newRef
		}
		m.records[newRef] = existing
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

// newTestObjectManager builds an ibclient.IBObjectManager pointed at the
// given httptest.Server via plain HTTP (no TLS needed — the
// WapiRequestBuilder only switches to HTTPS when hostCfg.Scheme !=
// "http").
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

	ref := m.seed("fixedaddress", &ibclient.FixedAddress{
		NetviewName: "default",
		IPv4Address: "10.0.0.5",
		Mac:         stringPtr("00:11:22:33:44:55"),
		MatchClient: stringPtr("MAC_ADDRESS"),
		Comment:     "hello",
		Ea:          ibclient.EA{"env": "prod"},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
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
	if cr.Status.AtProvider.IPv4Addr == nil || *cr.Status.AtProvider.IPv4Addr != "10.0.0.5" {
		t.Errorf("AtProvider.IPv4Addr = %v, want 10.0.0.5", cr.Status.AtProvider.IPv4Addr)
	}
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

// TestClusterObserveNotUpToDate verifies Observe() reports
// ResourceUpToDate=false when the desired spec drifts from the observed
// server state (as opposed to TestClusterObserveSuccess, which pins the
// matching case).
func TestClusterObserveNotUpToDate(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed("fixedaddress", &ibclient.FixedAddress{
		NetviewName: "default",
		IPv4Address: "10.0.0.5",
		Mac:         stringPtr("00:11:22:33:44:55"),
		MatchClient: stringPtr("MAC_ADDRESS"),
		Comment:     "old comment",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment") // drifted from server

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
	if got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=false for a comment drift, got true")
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", "fixedaddress/does-not-exist:10.0.0.5")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (empty strings, nil pointers, a
// nil Ea map, a nil Options slice) must not panic and must produce a
// valid observation with nil-safe AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed("fixedaddress", &ibclient.FixedAddress{})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", ref)

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
	if ap.IPv4Addr != nil {
		t.Errorf("AtProvider.IPv4Addr = %v, want nil", ap.IPv4Addr)
	}
	if ap.IPv6Addr != nil {
		t.Errorf("AtProvider.IPv6Addr = %v, want nil", ap.IPv6Addr)
	}
	if ap.MAC != nil {
		t.Errorf("AtProvider.MAC = %v, want nil", ap.MAC)
	}
	if ap.NetworkView != nil {
		t.Errorf("AtProvider.NetworkView = %v, want nil", ap.NetworkView)
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
	if ap.CloudInfo != nil {
		t.Errorf("AtProvider.CloudInfo = %v, want nil", ap.CloudInfo)
	}
}

// TestObservePreCreateState verifies that Observe short-circuits (no HTTP
// call) when the external-name still equals the CR's Kubernetes name —
// the pre-create state for a server-assigned external-name strategy.
func TestObservePreCreateState(t *testing.T) {
	// Zero-route server: any request is an error, proving Observe never
	// calls it during the pre-create guard.
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())              // simulate NameAsExternalName initializer

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
	cr := newClusterFixedAddress("my-fixedaddress", "fixedaddress/test1:10.0.0.5")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", "fixedaddress/test1:10.0.0.5")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
	if !strings.HasPrefix(got, "fixedaddress/") {
		t.Errorf("Create: external-name = %q, want fixedaddress/... (IPv4 object type)", got)
	}
}

// TestClusterCreateCapturesServerAssignedRef asserts the external-name
// annotation is set exactly to the _ref returned by the WAPI create
// response (server-assigned external-name strategy) via AllocateIP
// (NON-STANDARD — no CreateFixedAddress method exists).
func TestClusterCreateCapturesServerAssignedRef(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	m.mu.Lock()
	_, exists := m.records[got]
	m.mu.Unlock()
	if !exists {
		t.Errorf("Create: external-name %q does not match any server-side record", got)
	}
}

// TestClusterCreateIPv6 exercises the IPv6 object-type path (creation
// selects "ipv6fixedaddress" instead of "fixedaddress" based on which of
// ipv4addr/ipv6addr is set).
func TestClusterCreateIPv6(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddressIPv6("my-fixedaddress-v6", "")

	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if !strings.HasPrefix(got, "ipv6fixedaddress/") {
		t.Errorf("Create: external-name = %q, want ipv6fixedaddress/... (IPv6 object type)", got)
	}
}

// TestClusterCreateServerError verifies Create() surfaces a wrapped error
// (rather than a panic or silent success) when the WAPI backend rejects
// the POST request.
func TestClusterCreateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", "")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: want error for a 500 WAPI response, got nil")
	}

	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q after a failed create, want unset", got)
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed("fixedaddress", &ibclient.FixedAddress{
		NetviewName: "default",
		IPv4Address: "10.0.0.5",
		Mac:         stringPtr("00:11:22:33:44:55"),
		MatchClient: stringPtr("MAC_ADDRESS"),
		Comment:     "old comment",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %q, want %q", stored.Comment, "new comment")
	}
	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name changed to %q for a comment-only update, want unchanged %q", got, ref)
	}
}

// TestClusterUpdateRefreshesUnstableRef verifies the UNSTABLE _ref
// contract (ADR-IN-0004): when ipv4addr changes, Update() picks up the
// new _ref from the PUT response and refreshes the external-name
// annotation so subsequent reconciles use the correct reference.
func TestClusterUpdateRefreshesUnstableRef(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed("fixedaddress", &ibclient.FixedAddress{
		NetviewName: "default",
		IPv4Address: "10.0.0.5",
		Mac:         stringPtr("00:11:22:33:44:55"),
		MatchClient: stringPtr("MAC_ADDRESS"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", oldRef)
	cr.Spec.ForProvider.IPv4Addr = stringPtr("10.0.0.9")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == oldRef {
		t.Errorf("Update: external-name unchanged at %q after an ipv4addr-mutating update, want a new _ref", got)
	}
	m.mu.Lock()
	_, oldStillExists := m.records[oldRef]
	stored, newExists := m.records[got]
	m.mu.Unlock()
	if oldStillExists {
		t.Errorf("Update: old ref %q still present after _ref-mutating update", oldRef)
	}
	if !newExists || stored.IPv4Address != "10.0.0.9" {
		t.Errorf("Update: record at new ref %q = %+v, want ipv4addr 10.0.0.9", got, stored)
	}
}

// TestClusterUpdateDefaultsMatchClientForIPv4 pins the SDK quirk
// documented for this resource: UpdateFixedAddress rejects an empty
// match_client for IPv4 objects, so Update() must always send a concrete
// value even when spec.forProvider.matchClient is unset and
// status.atProvider.matchClient hasn't been populated yet (e.g. the very
// first Update immediately following Create, before any Observe has run).
func TestClusterUpdateDefaultsMatchClientForIPv4(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed("fixedaddress", &ibclient.FixedAddress{
		NetviewName: "default",
		IPv4Address: "10.0.0.5",
		MatchClient: stringPtr("MAC_ADDRESS"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", ref)
	cr.Spec.ForProvider.MatchClient = nil // unset — no AtProvider state populated either
	cr.Spec.ForProvider.Comment = stringPtr("triggers update")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.MatchClient == nil || *stored.MatchClient != matchClientDefault {
		t.Errorf("Update: stored match_client = %v, want default %q", stored.MatchClient, matchClientDefault)
	}
}

// TestClusterUpdateServerError verifies Update() surfaces a wrapped error
// (rather than a panic or a silently unchanged external-name) when the
// WAPI backend rejects the PUT request.
func TestClusterUpdateServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	ref := "fixedaddress/test1:10.0.0.5"
	cr := newClusterFixedAddress("my-fixedaddress", ref)
	cr.Spec.ForProvider.Comment = stringPtr("triggers update")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: want error for a 500 WAPI response, got nil")
	}

	if got := meta.GetExternalName(cr); got != ref {
		t.Errorf("Update: external-name changed to %q after a failed update, want unchanged %q", got, ref)
	}
}

// ── cluster: Delete ──────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed("fixedaddress", &ibclient.FixedAddress{IPv4Address: "10.0.0.5", NetviewName: "default"})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", ref)

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
	cr := newClusterFixedAddress("my-fixedaddress", "fixedaddress/does-not-exist:10.0.0.5")

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
	cr := newClusterFixedAddress("my-fixedaddress", "fixedaddress/test1:10.0.0.5")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteFixedAddress) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteFixedAddress)
	}
}

// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject verifies the
// core defect fix: a 404 against the stored _ref must not be treated as
// "already deleted" when a natural-key search finds the same identity
// still live under a different _ref. Deleting that record would be
// unverifiable ownership, so Delete() must refuse and leave the record
// in place.
func TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed("fixedaddress", &ibclient.FixedAddress{
		IPv4Address: "10.0.0.5",
		NetviewName: "default",
		Cidr:        "10.0.0.0/24",
		Mac:         stringPtr("00:11:22:33:44:55"),
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterFixedAddress("my-fixedaddress", "fixedaddress/stale-ref:10.0.0.5")
	cr.Spec.ForProvider.Network = stringPtr("10.0.0.0/24")

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
	cr := newClusterFixedAddress("my-fixedaddress", "fixedaddress/stale-ref:10.0.0.5")
	cr.Spec.ForProvider.Network = stringPtr("10.0.0.0/24")

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

	cr := newClusterFixedAddress("my-fixedaddress", "")
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

	cr := newClusterFixedAddress("my-fixedaddress", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed("fixedaddress", &ibclient.FixedAddress{
		NetviewName: "default",
		IPv4Address: "10.0.0.5",
		Mac:         stringPtr("00:11:22:33:44:55"),
		MatchClient: stringPtr("MAC_ADDRESS"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", ref, "ProviderConfig")

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
}

func TestNamespacedObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", "fixedaddress/does-not-exist:10.0.0.5", "ProviderConfig")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false for 404, got true")
	}
}

// TestNamespacedObserveMinimalResponse is the namespaced-scope counterpart
// of TestClusterObserveMinimalResponse: pins nil-safety in Observe when a
// WAPI response carries only the object's _ref and every other field at
// its Go zero value.
func TestNamespacedObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed("fixedaddress", &ibclient.FixedAddress{})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", ref, "ProviderConfig")

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
	if ap.IPv4Addr != nil {
		t.Errorf("AtProvider.IPv4Addr = %v, want nil", ap.IPv4Addr)
	}
	if ap.IPv6Addr != nil {
		t.Errorf("AtProvider.IPv6Addr = %v, want nil", ap.IPv6Addr)
	}
	if ap.MAC != nil {
		t.Errorf("AtProvider.MAC = %v, want nil", ap.MAC)
	}
	if ap.NetworkView != nil {
		t.Errorf("AtProvider.NetworkView = %v, want nil", ap.NetworkView)
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
	if ap.CloudInfo != nil {
		t.Errorf("AtProvider.CloudInfo = %v, want nil", ap.CloudInfo)
	}
}

func TestNamespacedObservePreCreateState(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", "", "ProviderConfig")
	meta.SetExternalName(cr, cr.GetName())

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if got.ResourceExists {
		t.Error("Observe: want ResourceExists=false in pre-create state, got true")
	}
}

// ── namespaced: Create ────────────────────────────────────────────────────

func TestNamespacedCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", "", "ProviderConfig")

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

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", "", "ProviderConfig")

	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("Create: want error for a 500 WAPI response, got nil")
	}
}

// ── namespaced: Update ─────────────────────────────────────────────────────

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed("fixedaddress", &ibclient.FixedAddress{
		NetviewName: "default",
		IPv4Address: "10.0.0.5",
		MatchClient: stringPtr("MAC_ADDRESS"),
		Comment:     "old comment",
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.records[ref]
	m.mu.Unlock()
	if stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %q, want %q", stored.Comment, "new comment")
	}
}

// ── namespaced: Delete ────────────────────────────────────────────────────

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed("fixedaddress", &ibclient.FixedAddress{IPv4Address: "10.0.0.5", NetviewName: "default"})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", ref, "ProviderConfig")

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
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", "fixedaddress/does-not-exist:10.0.0.5", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: want nil error for already-gone resource, got: %v", err)
	}
}

func TestNamespacedDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", "fixedaddress/test1:10.0.0.5", "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed("fixedaddress", &ibclient.FixedAddress{
		IPv4Address: "10.0.0.5",
		NetviewName: "default",
		Cidr:        "10.0.0.0/24",
		Mac:         stringPtr("00:11:22:33:44:55"),
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", "fixedaddress/stale-ref:10.0.0.5", "ProviderConfig")
	cr.Spec.ForProvider.Network = stringPtr("10.0.0.0/24")

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
	cr := newNamespacedFixedAddress("default", "my-fixedaddress", "fixedaddress/stale-ref:10.0.0.5", "ProviderConfig")
	cr.Spec.ForProvider.Network = stringPtr("10.0.0.0/24")

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

	cr := newNamespacedFixedAddress(ns, "my-fixedaddress", "", "ProviderConfig")
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

	cr := newNamespacedFixedAddress("app-ns", "my-fixedaddress", "", "ClusterProviderConfig")
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

	cr := newNamespacedFixedAddress("default", "my-fixedaddress", "", "SomeOtherKind")
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
		t.Errorf("ExtAttrs round-trip mismatch: in=%v out=%v", in, out)
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

func TestIsNotFoundClassifiesTypedError(t *testing.T) {
	err := &ibclient.NotFoundError{}
	if !isNotFound(err) {
		t.Error("isNotFound: want true for *ibclient.NotFoundError, got false")
	}
}

func TestIsNotFoundClassifiesGenericStatusError(t *testing.T) {
	if !isNotFound(errGenericStatus(404)) {
		t.Error("isNotFound: want true for a generic 404 status error, got false")
	}
	if isNotFound(errGenericStatus(500)) {
		t.Error("isNotFound: want false for a 500 status error, got true")
	}
}

func errGenericStatus(code int) error {
	return &genericStatusError{code: code}
}

type genericStatusError struct{ code int }

func (e *genericStatusError) Error() string {
	return "WAPI request error: " + itoa(e.code) + "('boom')\nContents:\n{}\n"
}

func TestDhcpOptionsRoundTrip(t *testing.T) {
	in := []dhcpOption{
		{Name: stringPtr("routers"), Num: int64Ptr(3), VendorClass: stringPtr("DHCP"), Value: stringPtr("10.0.0.1"), UseOption: boolPtr(true)},
	}
	sdk := toSDKOptions(in)
	out := dhcpOptionsFromSDK(sdk)
	if !dhcpOptionsEqual(in, out) {
		t.Errorf("DHCP options round-trip mismatch: in=%+v out=%+v", in, out)
	}
}

func TestDhcpOptionsEqualDetectsDiff(t *testing.T) {
	a := []dhcpOption{{Name: stringPtr("routers"), Value: stringPtr("10.0.0.1")}}
	b := []dhcpOption{{Name: stringPtr("routers"), Value: stringPtr("10.0.0.2")}}
	if dhcpOptionsEqual(a, b) {
		t.Error("dhcpOptionsEqual: want false for differing Value, got true")
	}
}

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	f := &fixedAddressFields{IPv4Addr: stringPtr("10.0.0.5")}
	fa := &ibclient.FixedAddress{
		IPv4Address: "10.0.0.5",
		NetviewName: "default",
		Comment:     "server comment",
		MatchClient: stringPtr("MAC_ADDRESS"),
		Mac:         stringPtr("00:11:22:33:44:55"),
		Ea:          ibclient.EA{"env": "prod"},
	}

	changed := lateInitialize(f, fa)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if strOrEmpty(f.NetworkView) != "default" {
		t.Errorf("lateInitialize: NetworkView = %v, want default", f.NetworkView)
	}
	if strOrEmpty(f.Comment) != "server comment" {
		t.Errorf("lateInitialize: Comment = %v, want %q", f.Comment, "server comment")
	}
	if strOrEmpty(f.MatchClient) != "MAC_ADDRESS" {
		t.Errorf("lateInitialize: MatchClient = %v, want MAC_ADDRESS", f.MatchClient)
	}
	if len(f.ExtAttrs) != 1 || f.ExtAttrs["env"] != "prod" {
		t.Errorf("lateInitialize: ExtAttrs = %v, want {env: prod}", f.ExtAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	f := &fixedAddressFields{
		IPv4Addr: stringPtr("10.0.0.5"),
		Comment:  stringPtr("user comment"),
	}
	fa := &ibclient.FixedAddress{
		IPv4Address: "10.0.0.5",
		Comment:     "server comment",
	}

	lateInitialize(f, fa)
	if strOrEmpty(f.Comment) != "user comment" {
		t.Errorf("lateInitialize: overwrote user-set Comment, got %v", f.Comment)
	}
}

// TestLateInitializeDoesNotBackfillOptionsWhenUseOptionsOff proves that
// when useOptions is false the observed DHCP options (WAPI's own default
// set, not values the user's config implies) are never written back into
// spec.forProvider.options.
func TestLateInitializeDoesNotBackfillOptionsWhenUseOptionsOff(t *testing.T) {
	f := &fixedAddressFields{
		IPv4Addr: stringPtr("10.0.0.5"),
	}
	fa := &ibclient.FixedAddress{
		IPv4Address: "10.0.0.5",
		UseOptions:  boolPtr(false),
		Options: []*ibclient.Dhcpoption{
			{Name: "routers", Value: "10.0.0.1"},
		},
	}

	lateInitialize(f, fa)

	if len(f.Options) != 0 {
		t.Errorf("lateInitialize: Options = %+v, want empty (useOptions is off, observed options are the server's own default set, not user values)", f.Options)
	}
}

func TestLateInitializeResolvesDynamicAllocationAddress(t *testing.T) {
	f := &fixedAddressFields{IPv4Addr: stringPtr(""), Network: stringPtr("10.0.0.0/24")}
	fa := &ibclient.FixedAddress{IPv4Address: "10.0.0.42", Cidr: "10.0.0.0/24"}

	changed := lateInitialize(f, fa)
	if !changed {
		t.Fatal("lateInitialize: want changed=true for dynamic-allocation backfill, got false")
	}
	if strOrEmpty(f.IPv4Addr) != "10.0.0.42" {
		t.Errorf("lateInitialize: IPv4Addr = %v, want the resolved literal address 10.0.0.42", f.IPv4Addr)
	}
}

// TestIsUpToDate is a table-driven test exercising every mutable field
// compared by isUpToDate (isUpToDateAddress and isUpToDateDHCP), covering
// both the matching baseline and a drift on each individual field.
func TestIsUpToDate(t *testing.T) {
	base := fixedAddressFields{
		IPv4Addr:                    stringPtr("10.0.0.5"),
		MAC:                         stringPtr("00:11:22:33:44:55"),
		NetworkView:                 stringPtr("default"),
		Network:                     stringPtr("10.0.0.0/24"),
		Name:                        stringPtr("host1"),
		MatchClient:                 stringPtr("MAC_ADDRESS"),
		Comment:                     stringPtr("hello"),
		ExtAttrs:                    map[string]string{"env": "prod"},
		Disable:                     boolPtr(false),
		AgentCircuitID:              stringPtr("circuit1"),
		AgentRemoteID:               stringPtr("remote1"),
		ClientIdentifierPrependZero: boolPtr(true),
		DHCPClientIdentifier:        stringPtr("client1"),
		UseOptions:                  boolPtr(true),
		Options: []dhcpOption{
			{Name: stringPtr("routers"), Value: stringPtr("10.0.0.1")},
		},
	}
	fa := &ibclient.FixedAddress{
		IPv4Address:                 "10.0.0.5",
		Mac:                         stringPtr("00:11:22:33:44:55"),
		NetviewName:                 "default",
		Cidr:                        "10.0.0.0/24",
		Name:                        stringPtr("host1"),
		MatchClient:                 stringPtr("MAC_ADDRESS"),
		Comment:                     "hello",
		Ea:                          ibclient.EA{"env": "prod"},
		Disable:                     boolPtr(false),
		AgentCircuitId:              stringPtr("circuit1"),
		AgentRemoteId:               stringPtr("remote1"),
		ClientIdentifierPrependZero: boolPtr(true),
		DhcpClientIdentifier:        stringPtr("client1"),
		UseOptions:                  boolPtr(true),
		Options: []*ibclient.Dhcpoption{
			{Name: "routers", Value: "10.0.0.1"},
		},
	}

	if !isUpToDate(base, fa) {
		t.Fatal("isUpToDate: want true for matching fields, got false")
	}

	cases := map[string]struct {
		reason string
		mutate func(f *fixedAddressFields)
	}{
		"IPv4Addr": {
			reason: "an ipv4addr diff must be detected",
			mutate: func(f *fixedAddressFields) { f.IPv4Addr = stringPtr("10.0.0.9") },
		},
		"MAC": {
			reason: "a mac diff must be detected",
			mutate: func(f *fixedAddressFields) { f.MAC = stringPtr("aa:bb:cc:dd:ee:ff") },
		},
		"NetworkView": {
			reason: "a network_view diff must be detected",
			mutate: func(f *fixedAddressFields) { f.NetworkView = stringPtr("other-view") },
		},
		"Network": {
			reason: "a network (CIDR) diff must be detected",
			mutate: func(f *fixedAddressFields) { f.Network = stringPtr("10.0.1.0/24") },
		},
		"Name": {
			reason: "a name diff must be detected",
			mutate: func(f *fixedAddressFields) { f.Name = stringPtr("host2") },
		},
		"MatchClient": {
			reason: "a match_client diff must be detected",
			mutate: func(f *fixedAddressFields) { f.MatchClient = stringPtr("CIRCUIT_ID") },
		},
		"Comment": {
			reason: "a comment diff must be detected",
			mutate: func(f *fixedAddressFields) { f.Comment = stringPtr("changed") },
		},
		"ExtAttrs": {
			reason: "an extattrs diff must be detected",
			mutate: func(f *fixedAddressFields) { f.ExtAttrs = map[string]string{"env": "dev"} },
		},
		"Disable": {
			reason: "a disable diff must be detected",
			mutate: func(f *fixedAddressFields) { f.Disable = boolPtr(true) },
		},
		"AgentCircuitID": {
			reason: "an agent_circuit_id diff must be detected",
			mutate: func(f *fixedAddressFields) { f.AgentCircuitID = stringPtr("circuit2") },
		},
		"AgentRemoteID": {
			reason: "an agent_remote_id diff must be detected",
			mutate: func(f *fixedAddressFields) { f.AgentRemoteID = stringPtr("remote2") },
		},
		"ClientIdentifierPrependZero": {
			reason: "a client_identifier_prepend_zero diff must be detected",
			mutate: func(f *fixedAddressFields) { f.ClientIdentifierPrependZero = boolPtr(false) },
		},
		"DHCPClientIdentifier": {
			reason: "a dhcp_client_identifier diff must be detected",
			mutate: func(f *fixedAddressFields) { f.DHCPClientIdentifier = stringPtr("client2") },
		},
		"UseOptions": {
			reason: "a use_options diff must be detected",
			mutate: func(f *fixedAddressFields) { f.UseOptions = boolPtr(false) },
		},
		"Options": {
			reason: "an options diff must be detected",
			mutate: func(f *fixedAddressFields) {
				f.Options = []dhcpOption{{Name: stringPtr("routers"), Value: stringPtr("10.0.0.2")}}
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			drifted := base
			tc.mutate(&drifted)
			if isUpToDate(drifted, fa) {
				t.Errorf("%s: isUpToDate: want false, got true", tc.reason)
			}
		})
	}
}

// TestIsUpToDateIgnoresOptionsWhenUseOptionsOff proves the options
// comparison is gated on useOptions. When it is false, WAPI ignores the
// submitted DHCP options and returns its own default set on every GET —
// the spec options and the observed options are unrelated, and comparing
// them unconditionally can never converge.
func TestIsUpToDateIgnoresOptionsWhenUseOptionsOff(t *testing.T) {
	f := fixedAddressFields{
		IPv4Addr:    stringPtr("10.0.0.5"),
		NetworkView: stringPtr("default"),
		UseOptions:  boolPtr(false),
		Options:     []dhcpOption{{Name: stringPtr("routers"), Value: stringPtr("10.0.0.2")}},
	}
	fa := &ibclient.FixedAddress{
		IPv4Address: "10.0.0.5",
		NetviewName: "default",
		UseOptions:  boolPtr(false),
		Options: []*ibclient.Dhcpoption{
			{Name: "routers", Value: "10.0.0.1"},
		},
	}
	if !isUpToDate(f, fa) {
		t.Error("isUpToDate: want true when useOptions is off and only the server-owned options differ, got false (non-convergent drift comparison)")
	}
}

// TestIsUpToDateDetectsUseOptionsTransition proves a useOptions
// true -> false transition is still detected as drift even though the
// value comparison is gated off. The flag comparison must be
// unconditional.
func TestIsUpToDateDetectsUseOptionsTransition(t *testing.T) {
	f := fixedAddressFields{
		IPv4Addr:    stringPtr("10.0.0.5"),
		NetworkView: stringPtr("default"),
		UseOptions:  boolPtr(false),
		Options:     []dhcpOption{{Name: stringPtr("routers"), Value: stringPtr("10.0.0.1")}},
	}
	fa := &ibclient.FixedAddress{
		IPv4Address: "10.0.0.5",
		NetviewName: "default",
		UseOptions:  boolPtr(true),
		Options: []*ibclient.Dhcpoption{
			{Name: "routers", Value: "10.0.0.1"},
		},
	}
	if isUpToDate(f, fa) {
		t.Error("isUpToDate: want false on a useOptions true -> false transition, got true (drift not detected)")
	}
}

// TestIsUpToDateIPv6Addr verifies the IPv6 branch of isUpToDateAddress:
// when the desired fields carry ipv6addr (isIPv6()==true), a mismatch on
// ipv6addr is detected instead of ipv4addr.
func TestIsUpToDateIPv6Addr(t *testing.T) {
	f := fixedAddressFields{IPv6Addr: stringPtr("2001:db8::5"), NetworkView: stringPtr("default")}
	fa := &ibclient.FixedAddress{IPv6Address: "2001:db8::5", NetviewName: "default"}
	if !isUpToDate(f, fa) {
		t.Fatal("isUpToDate: want true for matching ipv6addr, got false")
	}

	f.IPv6Addr = stringPtr("2001:db8::9")
	if isUpToDate(f, fa) {
		t.Error("isUpToDate: want false for an ipv6addr diff, got true")
	}
}

func TestMatchClientForUpdatePrefersDesired(t *testing.T) {
	got := matchClientForUpdate(stringPtr("CIRCUIT_ID"), stringPtr("MAC_ADDRESS"), false)
	if got != "CIRCUIT_ID" {
		t.Errorf("matchClientForUpdate = %q, want CIRCUIT_ID (desired takes priority)", got)
	}
}

func TestMatchClientForUpdateFallsBackToObserved(t *testing.T) {
	got := matchClientForUpdate(nil, stringPtr("REMOTE_ID"), false)
	if got != "REMOTE_ID" {
		t.Errorf("matchClientForUpdate = %q, want REMOTE_ID (observed fallback)", got)
	}
}

func TestMatchClientForUpdateDefaultsForIPv4(t *testing.T) {
	got := matchClientForUpdate(nil, nil, false)
	if got != matchClientDefault {
		t.Errorf("matchClientForUpdate = %q, want default %q", got, matchClientDefault)
	}
}

func TestMatchClientForUpdateIgnoredForIPv6(t *testing.T) {
	got := matchClientForUpdate(nil, nil, true)
	if got != "" {
		t.Errorf("matchClientForUpdate = %q, want empty string for IPv6 (no validation requirement)", got)
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
	secret := credentialsSecret("ns", "secret", "grid.example.com", "admin", "s3cr3t")
	secret.Data["ssl_verify"] = []byte("false")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "secret", Namespace: "ns"},
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
	if _, err := newObjectManagerWithScheme(&nioCredentials{Host: "example.com", Username: "u", Password: "p"}, false, "https", "443"); err != nil {
		t.Fatalf("newObjectManagerWithScheme: unexpected error: %v", err)
	}
}
