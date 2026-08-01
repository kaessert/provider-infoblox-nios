// Package networkcontainer unit tests for the NetworkContainer MR
// controllers. Tests use inline httptest.NewServer mocks that emulate the
// WAPI networkcontainer/ipv6networkcontainer endpoints, PascalCase test
// names (no underscores), and white-box access to the unexported
// connectors/clients so both scopes can be exercised without going
// through the full Connect() credential bridge on every test.
package networkcontainer

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/networkcontainer/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/networkcontainer/v1alpha1"
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

// Shared literal test fixtures, declared as named constants (rather than
// repeated inline string literals) to stay under the linter's
// minimum-occurrence threshold for repeated literals across this file.
const (
	testDefaultName      = "default" // provider config name / network view name / namespace / object name fixture
	testCIDR             = "10.0.0.0/16"
	testExtAttrKey       = "env"
	testExtAttrValue     = "prod"
	testUnusedKey        = "unused"
	testClusterNamespace = "crossplane-system"
)

func stringPtr(s string) *string { return &s }

func uintPtr(u uint) *uint { return &u }

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

// newClusterNetworkContainer builds a minimal cluster-scoped
// NetworkContainer CR. When externalName is empty, the external-name
// annotation is left unset. When it equals crName it simulates the
// framework's NameAsExternalName initializer (the pre-create state); any
// other value simulates a Create()-assigned server ref.
func newClusterNetworkContainer(crName, externalName string) *clusterv1alpha1.NetworkContainer {
	cr := &clusterv1alpha1.NetworkContainer{
		ObjectMeta: metav1.ObjectMeta{Name: crName, UID: "test-uid-cluster"},
		Spec: clusterv1alpha1.NetworkContainerSpec{
			ResourceSpec: xpv1.ResourceSpec{
				ProviderConfigReference: &xpv1.Reference{Name: testDefaultName},
			},
			ForProvider: clusterv1alpha1.NetworkContainerParameters{
				NetworkView: stringPtr(testDefaultName),
				Network:     stringPtr(testCIDR),
			},
		},
	}
	if externalName != "" {
		meta.SetExternalName(cr, externalName)
	}
	return cr
}

// newNamespacedNetworkContainer is the namespaced variant of
// newClusterNetworkContainer.
func newNamespacedNetworkContainer(ns, crName, externalName, pcKind string) *namespacedv1alpha1.NetworkContainer {
	cr := &namespacedv1alpha1.NetworkContainer{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns, UID: "test-uid-namespaced"},
		Spec: namespacedv1alpha1.NetworkContainerSpec{
			ManagedResourceSpec: xpv2.ManagedResourceSpec{
				ProviderConfigReference: &xpv1.ProviderConfigReference{Kind: pcKind, Name: testDefaultName},
			},
			ForProvider: namespacedv1alpha1.NetworkContainerParameters{
				NetworkView: stringPtr(testDefaultName),
				Network:     stringPtr(testCIDR),
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
// mockWapiServer emulates the subset of NIOS WAPI networkcontainer /
// ipv6networkcontainer endpoints exercised by the NetworkContainer
// controller (POST create — routed to one of the two object-type paths,
// GET/PUT/DELETE by _ref). Records are marshaled/unmarshaled using the
// real ibclient.NetworkContainer type so the wire format (including the
// EA {"value": ...} envelope) exactly matches what the SDK sends and
// expects.

type mockWapiServer struct {
	mu         sync.Mutex
	containers map[string]*ibclient.NetworkContainer
	nextRef    int

	// lastUpdateBody captures the raw JSON body of the most recent PUT
	// request, for tests that assert immutable fields are omitted.
	lastUpdateBody []byte
}

func newMockWapiServer() *mockWapiServer {
	return &mockWapiServer{containers: map[string]*ibclient.NetworkContainer{}}
}

// seed inserts nc directly (bypassing HTTP) and returns its _ref,
// assigning one if not already set. The object-type prefix (networkcontainer
// vs ipv6networkcontainer) is derived from whether Cidr looks like an IPv6
// network, mirroring isIPv6CIDR's colon heuristic.
func (m *mockWapiServer) seed(nc *ibclient.NetworkContainer) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRef++
	if nc.Ref == "" {
		nc.Ref = m.newRefLocked(nc)
	}
	m.containers[nc.Ref] = nc
	return nc.Ref
}

func (m *mockWapiServer) newRefLocked(nc *ibclient.NetworkContainer) string {
	prefix := "networkcontainer"
	if strings.Contains(nc.Cidr, ":") {
		prefix = "ipv6networkcontainer"
	}
	return prefix + "/test" + itoa(m.nextRef) + ":" + nc.Cidr + "/" + nc.NetviewName
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

// filterReturnFields emulates WAPI's _return_fields behavior: when the
// query parameter is present, the response includes only _ref plus the
// explicitly requested field names. When absent, the full object is
// returned unfiltered. This matters for
// TestClusterUpdateDoesNotSendImmutableField — UpdateNetworkContainer's
// internal merge GET requests only extattrs/comment (never
// network/network_view), so a mock that ignored _return_fields would let
// the immutable identity fields leak back into the PUT body via the
// merge, masking a real bug.
func filterReturnFields(nc *ibclient.NetworkContainer, returnFields string) interface{} {
	if returnFields == "" {
		return nc
	}
	raw, err := json.Marshal(nc)
	if err != nil {
		return nc
	}
	var full map[string]json.RawMessage
	if err := json.Unmarshal(raw, &full); err != nil {
		return nc
	}
	keep := map[string]bool{"_ref": true}
	for _, f := range strings.Split(returnFields, ",") {
		keep[f] = true
	}
	filtered := map[string]json.RawMessage{}
	for k, v := range full {
		if keep[k] {
			filtered[k] = v
		}
	}
	return filtered
}

// nextAvailableNetworkContainerRequest mirrors the wire shape of the SDK's
// NetworkContainerNextAvailable object — the request body both
// AllocateNetworkContainer and AllocateNetworkContainerByEA send. Both use
// a nested "network" object (carrying either a parentCidr search or an EA
// filter, plus the requested prefix length) rather than a plain CIDR
// string. Probing the raw "network" field's JSON type (object vs string)
// is what distinguishes an allocation request from a static-CIDR create in
// createHandler below.
type nextAvailableNetworkContainerRequest struct {
	Network *struct {
		ObjectParams map[string]string `json:"_object_parameters"`
		Params       map[string]uint   `json:"_parameters"`
	} `json:"network"`
	NetviewName string      `json:"network_view"`
	Comment     string      `json:"comment"`
	Ea          ibclient.EA `json:"extattrs"`
}

// handler returns an http.Handler implementing the networkcontainer /
// ipv6networkcontainer WAPI surface.
func (m *mockWapiServer) handler() http.Handler {
	mux := http.NewServeMux()

	createHandler := func(w http.ResponseWriter, r *http.Request) {
		body, err := readAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Probe the raw "network" field's JSON type: a nested object
		// means this is an AllocateNetworkContainer(ByEA) request; a
		// string (or absent) means a static-CIDR create.
		var probe struct {
			Network json.RawMessage `json:"network"`
		}
		if err := json.Unmarshal(body, &probe); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var nc ibclient.NetworkContainer
		switch {
		case len(probe.Network) > 0 && probe.Network[0] == '{':
			var req nextAvailableNetworkContainerRequest
			if err := json.Unmarshal(body, &req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var prefixLen uint
			if req.Network != nil {
				prefixLen = req.Network.Params["cidr"]
			}
			// The mock allocates from a fixed pool for the
			// parentCidr/filterParams paths — real WAPI would resolve
			// the search (parent CIDR match or EA match) against
			// actual container state server-side.
			nc = ibclient.NetworkContainer{
				NetviewName: req.NetviewName,
				Cidr:        "192.168.200.0/" + itoa(int(prefixLen)),
				Comment:     req.Comment,
				Ea:          req.Ea,
			}
		default:
			if err := json.Unmarshal(body, &nc); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		ref := m.seed(&nc)
		writeJSON(w, http.StatusOK, ref)
	}
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/networkcontainer", createHandler)
	mux.HandleFunc("POST /wapi/v"+wapiVersion+"/ipv6networkcontainer", createHandler)

	// Search endpoints (GetNetworkContainer): a GET with no _ref path
	// segment, filtered by network_view/network query params.
	// Registered as exact literal paths so Go's ServeMux prefers them
	// over the {ref...} wildcard below for requests to precisely
	// "networkcontainer"/"ipv6networkcontainer" (real _refs always
	// carry additional path segments).
	searchHandler := func(isIPv6 bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			networkView := q.Get("network_view")
			cidr := q.Get("network")

			m.mu.Lock()
			var matches []ibclient.NetworkContainer
			for ref, nc := range m.containers {
				refIsIPv6 := strings.HasPrefix(ref, "ipv6networkcontainer/")
				if refIsIPv6 != isIPv6 {
					continue
				}
				if networkView != "" && nc.NetviewName != networkView {
					continue
				}
				if cidr != "" && nc.Cidr != cidr {
					continue
				}
				matches = append(matches, *nc)
			}
			m.mu.Unlock()

			// Always respond 200 — WAPI search semantics report
			// "not found" via an empty array, never an HTTP error
			// status.
			writeJSON(w, http.StatusOK, matches)
		}
	}
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/networkcontainer", searchHandler(false))
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/ipv6networkcontainer", searchHandler(true))

	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		nc, ok := m.containers[ref]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, filterReturnFields(nc, r.URL.Query().Get("_return_fields")))
	})

	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		existing, ok := m.containers[ref]
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
		var incoming ibclient.NetworkContainer
		if err := json.Unmarshal(body, &incoming); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		m.lastUpdateBody = body
		// Only the mutable fields (comment, extattrs) are ever applied —
		// networkView/network stay untouched on the stored record,
		// mirroring WAPI's rejection of identity-field changes.
		existing.Comment = incoming.Comment
		existing.Ea = incoming.Ea
		m.mu.Unlock()

		writeJSON(w, http.StatusOK, ref)
	})

	mux.HandleFunc("DELETE /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		m.mu.Lock()
		_, ok := m.containers[ref]
		delete(m.containers, ref)
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

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
		Comment:     "hello",
		Ea:          ibclient.EA{testExtAttrKey: testExtAttrValue},
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)
	cr.Spec.ForProvider.Comment = stringPtr("hello")
	cr.Spec.ForProvider.ExtAttrs = map[string]string{testExtAttrKey: testExtAttrValue}

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
	if cr.Status.AtProvider.NetworkView == nil || *cr.Status.AtProvider.NetworkView != testDefaultName {
		t.Errorf("AtProvider.NetworkView = %v, want %q", cr.Status.AtProvider.NetworkView, testDefaultName)
	}
	if cr.Status.AtProvider.Network == nil || *cr.Status.AtProvider.Network != testCIDR {
		t.Errorf("AtProvider.Network = %v, want %q", cr.Status.AtProvider.Network, testCIDR)
	}
	if cond := cr.GetCondition(xpv1.TypeReady); cond.Status != corev1.ConditionTrue {
		t.Errorf("condition Ready = %v, want True", cond.Status)
	}
}

func TestClusterObserveLateInitializesNetworkAfterAllocation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	// Simulates a resource created via the parentCidr/filterParams
	// allocation path: spec.forProvider.network was never set by the
	// user, only the server knows the allocated CIDR.
	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        "10.0.5.0/16",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)
	cr.Spec.ForProvider.Network = nil
	cr.Spec.ForProvider.ParentCidr = stringPtr("10.0.0.0/8")
	cr.Spec.ForProvider.AllocatePrefixLen = uintPtr(16)

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: want ResourceLateInitialized=true after backfilling network from the allocated CIDR, got false")
	}
	if cr.Spec.ForProvider.Network == nil || *cr.Spec.ForProvider.Network != "10.0.5.0/16" {
		t.Errorf("Spec.ForProvider.Network = %v, want 10.0.5.0/16", cr.Spec.ForProvider.Network)
	}
}

func TestClusterObserveNotFound(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "networkcontainer/does-not-exist:10.0.0.0/16/default")

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
	cr := newClusterNetworkContainer("my-container", "") // external-name unset
	meta.SetExternalName(cr, cr.GetName())               // simulate NameAsExternalName initializer

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
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 500, got nil")
	}
}

func TestClusterObserveForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")

	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("Observe: expected error for 403, got nil")
	}
}

// TestClusterObserveMinimalResponse pins nil-safety in Observe: a WAPI
// response carrying only the object's _ref (the resource identifier) and
// every other field at its Go zero value (empty strings, a nil Ea map)
// must not panic and must produce a valid observation with nil-safe
// AtProvider fields.
func TestClusterObserveMinimalResponse(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)

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
	if ap.NetworkView != nil {
		t.Errorf("AtProvider.NetworkView = %v, want nil", ap.NetworkView)
	}
	if ap.Network != nil {
		t.Errorf("AtProvider.Network = %v, want nil", ap.Network)
	}
	if ap.Comment != nil {
		t.Errorf("AtProvider.Comment = %v, want nil", ap.Comment)
	}
	if ap.ExtAttrs != nil {
		t.Errorf("AtProvider.ExtAttrs = %v, want nil", ap.ExtAttrs)
	}
}

func TestClusterObserveIsUpToDateIgnoresImmutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)
	// Mutate the immutable networkView/network fields in spec — this must
	// NOT affect ResourceUpToDate, since they are excluded from
	// isUpToDate (WAPI has no UpdateNetworkContainer parameter for them).
	cr.Spec.ForProvider.NetworkView = stringPtr("changed-view")
	cr.Spec.ForProvider.Network = stringPtr("192.168.0.0/16")

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite networkView/network drift (immutable fields), got false")
	}
}

// TestClusterObserveIsUpToDateIgnoresAllocationFields verifies that
// parentCidr, allocatePrefixLen, and filterParams — create-time-only
// inputs to the allocation call, never echoed back by the WAPI response —
// never trigger a spurious Update. isUpToDate's signature does not even
// accept these fields, but this test guards that invariant explicitly so
// a future refactor cannot accidentally wire them in.
func TestClusterObserveIsUpToDateIgnoresAllocationFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)
	// Simulate drift on the allocation-only fields after creation — none
	// of these are ever part of the WAPI response, so none should affect
	// ResourceUpToDate.
	cr.Spec.ForProvider.ParentCidr = stringPtr("10.0.0.0/8")
	cr.Spec.ForProvider.AllocatePrefixLen = uintPtr(24)
	cr.Spec.ForProvider.FilterParams = map[string]string{"region": "us-east"}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true despite parentCidr/allocatePrefixLen/filterParams drift (allocation-only fields), got false")
	}
}

// ── cluster: Create ─────────────────────────────────────────────────────

func TestClusterCreateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "") // no external-name yet

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
	if !strings.HasPrefix(got, "networkcontainer/") {
		t.Errorf("Create: external-name = %q, want networkcontainer/ prefix for IPv4 CIDR", got)
	}
}

// TestClusterCreateSelectsIPv6ObjectType verifies that an IPv6 CIDR routes
// the create request to the ipv6networkcontainer object type, not
// networkcontainer.
func TestClusterCreateSelectsIPv6ObjectType(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "")
	cr.Spec.ForProvider.Network = stringPtr("2001:db8::/32")

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if !strings.HasPrefix(got, "ipv6networkcontainer/") {
		t.Errorf("Create: external-name = %q, want ipv6networkcontainer/ prefix for IPv6 CIDR", got)
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
	cr := newClusterNetworkContainer("my-container", "")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateNetworkContainer) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateNetworkContainer)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty after failed create", got)
	}
}

// ── cluster: Create — allocation paths ───────────────────────────────────

func TestClusterCreateAllocateFromParentCidr(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "")
	cr.Spec.ForProvider.Network = nil
	cr.Spec.ForProvider.ParentCidr = stringPtr("10.0.0.0/8")
	cr.Spec.ForProvider.AllocatePrefixLen = uintPtr(16)

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
	if !strings.HasPrefix(got, "networkcontainer/") {
		t.Errorf("Create: external-name = %q, want networkcontainer/ prefix", got)
	}
	if !strings.Contains(got, "/16/") {
		t.Errorf("Create: external-name = %q, want the allocated /16 subnet in the ref", got)
	}
}

func TestClusterCreateAllocateByFilterParams(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "")
	cr.Spec.ForProvider.Network = nil
	cr.Spec.ForProvider.FilterParams = map[string]string{"region": "us-east"}
	cr.Spec.ForProvider.AllocatePrefixLen = uintPtr(20)

	_, err := e.Create(context.Background(), cr)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}

	got := meta.GetExternalName(cr)
	if got == "" || got == cr.GetName() {
		t.Errorf("Create: external-name not set to server-assigned ref, got %q", got)
	}
	if !strings.Contains(got, "/20/") {
		t.Errorf("Create: external-name = %q, want the allocated /20 subnet in the ref", got)
	}
}

func TestCreateValidationRejectsParentCidrAndFilterParams(t *testing.T) {
	_, err := createOrAllocateNetworkContainer(nil, stringPtr(testDefaultName), nil, stringPtr("10.0.0.0/8"), nil, uintPtr(16), map[string]string{"region": "us-east"}, nil)
	if err == nil {
		t.Fatal("createOrAllocateNetworkContainer: want error when parentCidr and filterParams are both set, got nil")
	}
}

func TestCreateValidationRequiresAllocatePrefixLenForParentCidr(t *testing.T) {
	_, err := createOrAllocateNetworkContainer(nil, stringPtr(testDefaultName), nil, stringPtr("10.0.0.0/8"), nil, nil, nil, nil)
	if err == nil {
		t.Fatal("createOrAllocateNetworkContainer: want error when parentCidr is set without allocatePrefixLen, got nil")
	}
}

func TestCreateValidationRequiresAllocatePrefixLenForFilterParams(t *testing.T) {
	_, err := createOrAllocateNetworkContainer(nil, stringPtr(testDefaultName), nil, nil, nil, nil, map[string]string{"region": "us-east"}, nil)
	if err == nil {
		t.Fatal("createOrAllocateNetworkContainer: want error when filterParams is set without allocatePrefixLen, got nil")
	}
}

// ── cluster: Update ─────────────────────────────────────────────────────

func TestClusterUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
		Comment:     "old comment",
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.containers[ref]
	m.mu.Unlock()
	if stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %q, want %q", stored.Comment, "new comment")
	}
}

func TestUpdateDoesNotSendImmutableField(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
	})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)

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
	for _, immutable := range []string{"network", "network_view"} {
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
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateNetworkContainer) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateNetworkContainer)
	}
}

// ── cluster: Delete ─────────────────────────────────────────────────────

func TestClusterDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{NetviewName: testDefaultName, Cidr: testCIDR})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", ref)

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.containers[ref]
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
	cr := newClusterNetworkContainer("my-container", "networkcontainer/does-not-exist:10.0.0.0/16/default")

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
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteNetworkContainer) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteNetworkContainer)
	}
}

func TestClusterDeleteForbidden(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusForbidden))
	defer srv.Close()

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "networkcontainer/test1:10.0.0.0/16/default")

	if _, err := e.Delete(context.Background(), cr); err == nil {
		t.Fatal("Delete: expected error for 403, got nil")
	}
}

// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject verifies the
// core defect fix: a 404 against the stored _ref must not be treated as
// "already deleted" when a natural-key search finds the same identity
// still live under a different _ref. Deleting that container would be
// unverifiable ownership, so Delete() must refuse and leave the record
// in place.
func TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.NetworkContainer{NetviewName: testDefaultName, Cidr: testCIDR})

	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newClusterNetworkContainer("my-container", "networkcontainer/stale-ref:"+testCIDR+"/"+testDefaultName)

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.containers[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live container was removed despite the refusal — DELETE must not have been issued against it")
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
	cr := newClusterNetworkContainer("my-container", "networkcontainer/stale-ref:"+testCIDR+"/"+testDefaultName)

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
		ns     = testClusterNamespace
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&clusterpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: testDefaultName},
				Spec: clusterpcv1alpha1.ProviderConfigSpec{
					Credentials: clusterpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             testUnusedKey,
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

	cr := newClusterNetworkContainer("my-container", "")
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

	cr := newClusterNetworkContainer("my-container", "")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for missing ProviderConfig, got nil")
	}
}

// ── namespaced: Observe ──────────────────────────────────────────────────

func TestNamespacedObserveSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", ref, "ProviderConfig")

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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/does-not-exist:10.0.0.0/16/default", "ProviderConfig")

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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "", "ProviderConfig")
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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/test1:10.0.0.0/16/default", "ProviderConfig")

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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "", "ProviderConfig")

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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "", "ProviderConfig")

	_, err := e.Create(context.Background(), cr)
	if err == nil {
		t.Fatal("Create: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errCreateNetworkContainer) {
		t.Errorf("Create: error = %q, want it to contain %q (wrapped, not swallowed)", got, errCreateNetworkContainer)
	}
	if got := meta.GetExternalName(cr); got != "" {
		t.Errorf("Create: external-name = %q, want empty after failed create", got)
	}
}

func TestNamespacedUpdateSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
		Comment:     "old comment",
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", ref, "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	m.mu.Lock()
	stored := m.containers[ref]
	m.mu.Unlock()
	if stored.Comment != "new comment" {
		t.Errorf("Update: stored comment = %q, want %q", stored.Comment, "new comment")
	}
}

// TestNamespacedUpdateError verifies that a WAPI 5xx response during
// Update is propagated (wrapped, not swallowed).
func TestNamespacedUpdateError(t *testing.T) {
	srv := httptest.NewServer(fixedStatusHandler(http.StatusInternalServerError))
	defer srv.Close()

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/test1:10.0.0.0/16/default", "ProviderConfig")
	cr.Spec.ForProvider.Comment = stringPtr("new comment")

	_, err := e.Update(context.Background(), cr)
	if err == nil {
		t.Fatal("Update: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errUpdateNetworkContainer) {
		t.Errorf("Update: error = %q, want it to contain %q (wrapped, not swallowed)", got, errUpdateNetworkContainer)
	}
}

// TestNamespacedUpdateDoesNotSendImmutableFields mirrors the cluster-scope
// assertion: network and network_view must never appear in the
// namespaced-scope Update request body either.
func TestNamespacedUpdateDoesNotSendImmutableFields(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{
		NetviewName: testDefaultName,
		Cidr:        testCIDR,
	})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", ref, "ProviderConfig")

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
	for _, immutable := range []string{"network", "network_view"} {
		if _, present := raw[immutable]; present {
			t.Errorf("Update: request body contains immutable field %q: %v", immutable, raw[immutable])
		}
	}
}

func TestNamespacedDeleteSuccess(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	ref := m.seed(&ibclient.NetworkContainer{NetviewName: testDefaultName, Cidr: testCIDR})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", ref, "ProviderConfig")

	if _, err := e.Delete(context.Background(), cr); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}

	m.mu.Lock()
	_, stillExists := m.containers[ref]
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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/does-not-exist:10.0.0.0/16/default", "ProviderConfig")

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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/test1:10.0.0.0/16/default", "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected error for 500, got nil")
	}
	if got := err.Error(); !strings.Contains(got, errDeleteNetworkContainer) {
		t.Errorf("Delete: error = %q, want it to contain %q (wrapped, not swallowed)", got, errDeleteNetworkContainer)
	}
}

// TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject is the
// namespaced-scope counterpart of
// TestClusterDeleteRefusesWhenStaleRefStillMatchesLiveObject.
func TestNamespacedDeleteRefusesWhenStaleRefStillMatchesLiveObject(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	liveRef := m.seed(&ibclient.NetworkContainer{NetviewName: testDefaultName, Cidr: testCIDR})

	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/stale-ref:"+testCIDR+"/"+testDefaultName, "ProviderConfig")

	_, err := e.Delete(context.Background(), cr)
	if err == nil {
		t.Fatal("Delete: expected refusal error when a natural-key search still matches a live object, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("Delete: error = %q, want it to explain the refusal", err.Error())
	}

	m.mu.Lock()
	_, stillExists := m.containers[liveRef]
	m.mu.Unlock()
	if !stillExists {
		t.Error("Delete: live container was removed despite the refusal — DELETE must not have been issued against it")
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
	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "networkcontainer/stale-ref:"+testCIDR+"/"+testDefaultName, "ProviderConfig")

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
		ns     = testDefaultName
		secret = "infobloxnios-api-key"
	)

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: testDefaultName, Namespace: ns},
				Spec: namespacedpcv1alpha1.ProviderConfigSpec{
					Credentials: namespacedpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             testUnusedKey,
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

	cr := newNamespacedNetworkContainer(ns, "my-container", "", "ProviderConfig")
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
	ns := testClusterNamespace

	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			credentialsSecret(ns, secret, "grid.example.com", "admin", "s3cr3t"),
			&namespacedpcv1alpha1.ClusterProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: testDefaultName},
				Spec: namespacedpcv1alpha1.ProviderConfigSpec{
					Credentials: namespacedpcv1alpha1.ProviderCredentials{
						Source: xpv1.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
							SecretRef: &xpv1.SecretKeySelector{
								SecretReference: xpv1.SecretReference{Name: secret, Namespace: ns},
								Key:             testUnusedKey,
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

	cr := newNamespacedNetworkContainer("app-ns", "my-container", "", "ClusterProviderConfig")
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

	cr := newNamespacedNetworkContainer(testDefaultName, "my-container", "", "SomeOtherKind")
	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for unsupported provider config kind, got nil")
	}
}

// ── shared helper unit tests ─────────────────────────────────────────────

// TestStringifyEAValue exercises every branch of the extensible-attribute
// value renderer: the plain string fast path exercised elsewhere via
// TestExtAttrsRoundTrip only covers the string case, so this test pins
// down the nil, ibclient.Bool (both true and false), []string, and
// default (numeric) cases too.
func TestStringifyEAValue(t *testing.T) {
	cases := map[string]struct {
		reason string
		in     interface{}
		want   string
	}{
		"Nil": {
			reason: "a nil EA value renders as an empty string",
			in:     nil,
			want:   "",
		},
		"String": {
			reason: "a string value passes through unchanged",
			in:     "prod",
			want:   "prod",
		},
		"BoolTrue": {
			reason: "ibclient.Bool(true) renders as the CRD's \"True\" literal",
			in:     ibclient.Bool(true),
			want:   "True",
		},
		"BoolFalse": {
			reason: "ibclient.Bool(false) renders as the CRD's \"False\" literal",
			in:     ibclient.Bool(false),
			want:   "False",
		},
		"StringSlice": {
			reason: "a []string value (as produced by EA.UnmarshalJSON for multi-value EAs) joins on commas",
			in:     []string{"a", "b", "c"},
			want:   "a,b,c",
		},
		"StringSliceEmpty": {
			reason: "an empty []string renders as an empty string",
			in:     []string{},
			want:   "",
		},
		"DefaultInt": {
			reason: "any other type (e.g. int) falls through to the default fmt.Sprintf branch",
			in:     42,
			want:   "42",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := stringifyEAValue(tc.in); got != tc.want {
				t.Errorf("%s: stringifyEAValue(%#v) = %q, want %q", tc.reason, tc.in, got, tc.want)
			}
		})
	}
}

func TestExtAttrsRoundTrip(t *testing.T) {
	in := map[string]string{testExtAttrKey: testExtAttrValue, "owner": "platform-team"}
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

func TestIsIPv6CIDR(t *testing.T) {
	cases := map[string]struct {
		cidr string
		want bool
	}{
		"IPv4Cidr":       {cidr: testCIDR, want: false},
		"IPv4Slash32":    {cidr: "192.168.1.1/32", want: false},
		"IPv6Cidr":       {cidr: "2001:db8::/32", want: true},
		"IPv6Slash128":   {cidr: "::1/128", want: true},
		"MalformedColon": {cidr: "not-a-cidr:still-has-colon", want: true},
		"MalformedPlain": {cidr: "not-a-cidr", want: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := isIPv6CIDR(tc.cidr); got != tc.want {
				t.Errorf("isIPv6CIDR(%q) = %v, want %v", tc.cidr, got, tc.want)
			}
		})
	}
}

func TestLateInitializeBackfillsOptionalFields(t *testing.T) {
	var network *string
	var comment *string
	extAttrs := map[string]string(nil)

	nc := &ibclient.NetworkContainer{
		Cidr:    "10.0.0.0/16",
		Comment: "server default",
		Ea:      ibclient.EA{testExtAttrKey: testExtAttrValue},
	}

	changed := lateInitialize(&network, &comment, &extAttrs, nc)
	if !changed {
		t.Fatal("lateInitialize: want changed=true, got false")
	}
	if network == nil || *network != "10.0.0.0/16" {
		t.Errorf("lateInitialize: network = %v, want %q", network, "10.0.0.0/16")
	}
	if comment == nil || *comment != "server default" {
		t.Errorf("lateInitialize: comment = %v, want %q", comment, "server default")
	}
	if !extAttrsEqual(extAttrs, map[string]string{testExtAttrKey: testExtAttrValue}) {
		t.Errorf("lateInitialize: extAttrs = %v, want {env: prod}", extAttrs)
	}
}

func TestLateInitializeDoesNotOverwriteSetFields(t *testing.T) {
	network := stringPtr("10.0.0.0/16")
	comment := stringPtr("user comment")
	extAttrs := map[string]string{testExtAttrKey: "staging"}

	nc := &ibclient.NetworkContainer{
		Cidr:    "10.0.1.0/16",
		Comment: "server default",
		Ea:      ibclient.EA{testExtAttrKey: testExtAttrValue},
	}

	changed := lateInitialize(&network, &comment, &extAttrs, nc)
	if changed {
		t.Error("lateInitialize: want changed=false when all fields already set, got true")
	}
	if *network != "10.0.0.0/16" {
		t.Errorf("lateInitialize: network = %q, want unchanged %q", *network, "10.0.0.0/16")
	}
	if *comment != "user comment" {
		t.Errorf("lateInitialize: comment = %q, want unchanged %q", *comment, "user comment")
	}
	if !extAttrsEqual(extAttrs, map[string]string{testExtAttrKey: "staging"}) {
		t.Errorf("lateInitialize: extAttrs = %v, want unchanged {env: staging}", extAttrs)
	}
}

func TestIsUpToDateDetectsCommentDrift(t *testing.T) {
	nc := &ibclient.NetworkContainer{Comment: "current"}
	if isUpToDate(stringPtr("different"), nil, nc) {
		t.Error("isUpToDate: want false when comment differs, got true")
	}
	if !isUpToDate(stringPtr("current"), nil, nc) {
		t.Error("isUpToDate: want true when comment matches, got false")
	}
}

func TestIsUpToDateDetectsExtAttrsDrift(t *testing.T) {
	nc := &ibclient.NetworkContainer{Ea: ibclient.EA{testExtAttrKey: testExtAttrValue}}
	if isUpToDate(nil, map[string]string{testExtAttrKey: "staging"}, nc) {
		t.Error("isUpToDate: want false when extAttrs differ, got true")
	}
	if !isUpToDate(nil, map[string]string{testExtAttrKey: testExtAttrValue}, nc) {
		t.Error("isUpToDate: want true when extAttrs match, got false")
	}
}

// ── extractCredentials ────────────────────────────────────────────────────

func TestExtractCredentialsMissingSecretRef(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	_, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, nil, "")
	if err == nil {
		t.Fatal("extractCredentials: expected error for nil secretRef, got nil")
	}
}

func TestExtractCredentialsUnsupportedSource(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	_, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceNone, nil, "")
	if err == nil {
		t.Fatal("extractCredentials: expected error for unsupported credentials source, got nil")
	}
}

func TestExtractCredentialsMissingKeys(t *testing.T) {
	scheme := newTestScheme(t)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "infobloxnios-credentials", Namespace: testClusterNamespace},
		Data:       map[string][]byte{"host": []byte("grid.example.com")},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	_, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: testClusterNamespace},
		Key:             testUnusedKey,
	}, "")
	if err == nil {
		t.Fatal("extractCredentials: expected error for missing username/password keys, got nil")
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
	secret := credentialsSecret(testClusterNamespace, "infobloxnios-credentials", "grid.example.com", "admin", "s3cr3t")
	secret.Data["ssl_verify"] = []byte("false")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := extractCredentials(context.Background(), kube, xpv1.CredentialsSourceSecret, &xpv1.SecretKeySelector{
		SecretReference: xpv1.SecretReference{Name: "infobloxnios-credentials", Namespace: testClusterNamespace},
		Key:             testUnusedKey,
	}, "")
	if err != nil {
		t.Fatalf("extractCredentials: unexpected error: %v", err)
	}
	if creds.Host != "grid.example.com" || creds.Username != "admin" || creds.Password != "s3cr3t" {
		t.Errorf("extractCredentials: got %+v, want Host/Username/Password populated regardless of the ssl_verify key", creds)
	}
}
